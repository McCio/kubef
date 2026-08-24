use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use futures::StreamExt;
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{Pod, Service},
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{
    Api, Client,
    api::PartialObjectMeta,
    client::scope::Namespace,
    core::Selector,
    runtime::{
        PredicateConfig, WatchStreamExt, predicates,
        reflector::{self, ReflectHandle, Store},
        watcher,
    },
};
use tokio::task::JoinHandle;
use tracing::debug;

use crate::cnf::{
    self,
    schema::{Config, PortMapping, PortSpec, Resource, ResourceSelector, SelectorPolicy},
};

type Object = PartialObjectMeta<Pod>;

pub struct Watcher {
    store: Store<Object>,
    subscriber: ReflectHandle<Object>,
    counter: AtomicUsize,
    policy: SelectorPolicy,
    handle: JoinHandle<()>,
}

impl Watcher {
    pub async fn new(api: Api<PartialObjectMeta<Pod>>, selector: &Selector, policy: SelectorPolicy) -> Result<Self> {
        let (store, writer) = reflector::store_shared(256);

        let config = watcher::Config::default().labels_from(selector);
        let subscriber = writer.subscribe().context("Failed to create subscriber")?;

        let handle = tokio::spawn(
            watcher::watcher(api, config)
                .reflect(writer)
                .default_backoff()
                .applied_objects()
                .predicate_filter(predicates::labels, PredicateConfig::default())
                .for_each(|_| async {}),
        );

        tokio::time::timeout(Duration::from_secs(10), store.wait_until_ready())
            .await
            .context("Timeout waiting for pods")?
            .context("Failed to wait for pods")?;

        Ok(Self {
            store,
            subscriber,
            counter: AtomicUsize::new(0),
            policy,
            handle,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    pub fn get(&self) -> Option<Arc<Object>> {
        if self.store.is_empty() {
            return None;
        }

        let state = self.store.state();
        let counter = match self.policy {
            SelectorPolicy::Sticky => self.counter.load(Ordering::Relaxed),
            SelectorPolicy::RoundRobin => self.counter.fetch_add(1, Ordering::Relaxed),
        };

        let index = if state.len().is_power_of_two() {
            counter & (state.len() - 1)
        } else {
            counter % state.len()
        };

        debug!("Selecting pod {} of {}", index, state.len());

        state.get(index).cloned()
    }

    pub async fn next(&mut self) -> Result<Arc<Object>> {
        self.subscriber.next().await.context("Cannot get next pod")
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Resolve the concrete container port number for `resource`, consulting the Kubernetes API when
/// needed (named ports or service mapping).
pub async fn resolve_port(client: &Client, resource: &Resource, config: &Config) -> Result<u16> {
    let effective_mapping = resource
        .ports
        .mapping
        .or_else(|| config.ports.as_ref().map(|gp| gp.mapping))
        .unwrap_or_default();

    let namespace = cnf::resolve_namespace(resource, config);

    match &resource.ports.remote {
        PortSpec::Number(n) => {
            if effective_mapping == PortMapping::Service {
                let ResourceSelector::Service(name) = &resource.selector else {
                    anyhow::bail!("service mapping requires service selector");
                };

                let service = client
                    .get::<Service>(name, &Namespace::from(namespace))
                    .await?;

                let spec = service.spec.context("Service has no spec")?;
                let port = spec
                    .ports
                    .as_deref()
                    .and_then(|ports| ports.iter().find(|p| p.port == i32::from(*n)))
                    .context("Service port not found")?;

                resolve_target_port(
                    port.target_port
                        .as_ref()
                        .context("Service port has no targetPort")?,
                )
            } else {
                Ok(*n)
            }
        }
        PortSpec::Named(name) => {
            let ResourceSelector::Service(svc_name) = &resource.selector else {
                anyhow::bail!("named port resolution requires service selector");
            };

            let service = client
                .get::<Service>(svc_name, &Namespace::from(namespace))
                .await?;

            let spec = service.spec.context("Service has no spec")?;
            let port = spec
                .ports
                .as_deref()
                .and_then(|ports| ports.iter().find(|p| p.name.as_deref() == Some(name)))
                .context("Named service port not found")?;

            resolve_target_port(
                port.target_port
                    .as_ref()
                    .context("Service port has no targetPort")?,
            )
        }
    }
}

fn resolve_target_port(target: &IntOrString) -> Result<u16> {
    match target {
        IntOrString::Int(n) => Ok(u16::try_from(*n).context("targetPort out of u16 range")?),
        IntOrString::String(_) => anyhow::bail!("named targetPort not yet supported"),
    }
}

pub async fn select(client: &Client, resource: &Resource, config: &Config) -> Result<Selector> {
    let namespace = cnf::resolve_namespace(resource, config);
    match &resource.selector {
        ResourceSelector::Label(labels) => Ok(Selector::from_iter(labels.clone())),
        ResourceSelector::Deployment(name) => {
            let deployment = client
                .get::<Deployment>(name, &Namespace::from(namespace))
                .await?;

            let selector = deployment
                .spec
                .context("Deployment has no spec")?
                .selector
                .try_into()?;

            Ok(selector)
        }
        ResourceSelector::Service(name) => {
            let service = client
                .get::<Service>(name, &Namespace::from(namespace))
                .await?;

            let selector = service
                .spec
                .context("Service has no spec")?
                .selector
                .context("Service has no selector")?;

            // TODO: it's a hack, kube-rs does something horrible behind the scenes
            Ok(Selector::from_iter(selector))
        }
    }
}
