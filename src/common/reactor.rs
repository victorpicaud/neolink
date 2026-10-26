//! This is the highest level to a camera
//! it represents a collection of managed cameras
use anyhow::anyhow;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::{
    sync::{
        mpsc::{channel as mpsc, Sender as MpscSender},
        oneshot::{channel as oneshot, Sender as OneshotSender},
        watch::{channel as watch, Receiver as WatchReceiver},
    },
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use super::{NeoCam, NeoInstance};
#[cfg(feature = "pushnoti")]
use crate::common::PushNotiThread;
use crate::{config::Config, AnyResult, Result};

#[allow(clippy::large_enum_variant)]
enum NeoReactorCommand {
    HangUp,
    /// Stops every camera, logged out, then the reactor
    Shutdown(OneshotSender<()>),
    Config(OneshotSender<WatchReceiver<Config>>),
    UpdateConfig(Config, OneshotSender<Result<()>>),
    Get(String, OneshotSender<Result<Option<NeoInstance>>>),
}

/// Reactor handles the collection of cameras
#[derive(Clone)]
pub(crate) struct NeoReactor {
    cancel: CancellationToken,
    commander: MpscSender<NeoReactorCommand>,
    set: Option<Arc<JoinSet<AnyResult<()>>>>,
}

impl NeoReactor {
    pub(crate) async fn new(config: Config) -> Self {
        let (commad_tx, mut command_rx) = mpsc(100);
        #[cfg(feature = "pushnoti")]
        let (push_noti, mut pn_rx) = mpsc(100);
        #[cfg(feature = "pushnoti")]
        let pn_tx = push_noti.clone();
        let cancel = CancellationToken::new();
        let (config_tx, _) = watch(config);
        let mut set = JoinSet::new();
        let config_tx = Arc::new(config_tx);

        let cancel1 = cancel.clone();
        let cancel2 = cancel.clone();
        let thread_config_tx = config_tx.clone();
        set.spawn(async move {
            let mut instances: HashMap<String, NeoCam> = Default::default();

            let r = tokio::select! {
                _ = cancel1.cancelled() => {
                    instances.clear();
                    Ok(())
                },
                v = async {
                    while let Some(command) = command_rx.recv().await {
                        match command {
                            NeoReactorCommand::HangUp =>  {
                                instances.clear();
                                cancel2.cancel();
                                return Result::<(), anyhow::Error>::Ok(());
                            }
                            NeoReactorCommand::Shutdown(reply) => {
                                // All at once, as each may have to wait for
                                // its camera
                                let mut stopping = JoinSet::new();
                                for (_, camera) in instances.drain() {
                                    stopping.spawn(camera.shutdown());
                                }
                                while stopping.join_next().await.is_some() {}
                                cancel2.cancel();
                                let _ = reply.send(());
                                return Ok(());
                            }
                            NeoReactorCommand::Config(reply) =>  {
                                let _ = reply.send(thread_config_tx.subscribe());
                            }
                            NeoReactorCommand::Get(name, sender) => {
                                // A camera whose actor ended answers nothing:
                                // it starts again instead
                                if instances.get(&name).is_some_and(NeoCam::is_dead) {
                                    log::info!("{name}: Starting the camera again");
                                    instances.remove(&name);
                                }
                                // A camera that fails to start fails the
                                // request, and no longer every camera with it
                                let new = match instances.entry(name.clone()) {
                                    Entry::Occupied(occ) => occ.get().subscribe().await.map(Some),
                                    Entry::Vacant(vac) => {
                                        let current_config: Config = (*thread_config_tx.borrow()).clone();
                                        if let Some(config) = current_config.cameras.iter().find(|cam| cam.name == name).cloned() {
                                            #[cfg(feature = "pushnoti")]
                                            let cam = NeoCam::new(config, push_noti.clone()).await;
                                            #[cfg(not(feature = "pushnoti"))]
                                            let cam = NeoCam::new(config).await;
                                            match cam {
                                                Ok(cam) => vac.insert(cam).subscribe().await.map(Some),
                                                Err(e) => Err(e),
                                            }
                                        } else {
                                            Result::Ok(None)
                                        }
                                    }
                                };
                                let _ = sender.send(new);
                            },
                            NeoReactorCommand::UpdateConfig(new_conf, reply) => {
                                // Shutdown or Notify instances of a change
                                let mut names = new_conf.cameras.iter().filter(|cam_conf| cam_conf.enabled).map(|cam_conf| (cam_conf.name.clone(), cam_conf.clone())).collect::<HashMap<_,_>>();
                                // Set the new conf first, so that whatever
                                // runs a removed camera learns it was removed
                                // before the camera goes away under it
                                let _ = thread_config_tx.send_replace(new_conf);
                                // Remove those no longer in the config
                                instances.retain(|name, _| names.contains_key(name));
                                for (name, instance) in instances.iter() {
                                    if let Some(conf) = names.remove(name) {
                                        let _ = instance.update_config(conf).await;
                                    }
                                }

                                // Reply that we are done
                                let _ = reply.send(Ok(()));
                            }
                        }
                    }
                    Ok(())
                } => v,
            };
            r
        });

        // Push notification client
        #[cfg(feature = "pushnoti")]
        {
            let cancel1 = cancel.clone();
            let thread_config_rx = config_tx.subscribe();
            set.spawn(async move {
                let pn = PushNotiThread::new().await?;
                tokio::select! {
                    _ = cancel1.cancelled() => AnyResult::Ok(()),
                    v = pn.run(thread_config_rx, pn_tx, &mut pn_rx) => v,
                }
            });
        }

        Self {
            cancel,
            commander: commad_tx,
            set: Some(Arc::new(set)),
        }
    }

    /// Get camera by name but do not create
    pub(crate) async fn get(&self, name: &str) -> Result<NeoInstance> {
        let (sender_tx, sender_rx) = oneshot();
        self.commander
            .send(NeoReactorCommand::Get(name.to_string(), sender_tx))
            .await?;

        sender_rx
            .await??
            .ok_or(anyhow!("Camera `{name}` not found in config"))
    }

    pub(crate) async fn config(&self) -> Result<WatchReceiver<Config>> {
        let (sender_tx, sender_rx) = oneshot();
        self.commander
            .send(NeoReactorCommand::Config(sender_tx))
            .await?;

        Ok(sender_rx.await?)
    }

    pub(crate) async fn update_config(&self, new_config: Config) -> Result<()> {
        let (sender_tx, sender_rx) = oneshot();
        self.commander
            .send(NeoReactorCommand::UpdateConfig(new_config, sender_tx))
            .await?;

        sender_rx.await?
    }

    /// Stops every camera, logged out. Cameras that were only dropped as
    /// neolink exited never logged out: dropping left that to tasks that
    /// exiting cut short.
    pub(crate) async fn shutdown(&self) {
        let (sender_tx, sender_rx) = oneshot();
        if self
            .commander
            .send(NeoReactorCommand::Shutdown(sender_tx))
            .await
            .is_ok()
        {
            let _ = sender_rx.await;
        }
    }
}

impl Drop for NeoReactor {
    fn drop(&mut self) {
        if let Some(set) = self.set.take() {
            if let Ok(mut set) = Arc::try_unwrap(set) {
                log::trace!("Drop NeoReactor");
                self.cancel.cancel();
                let commander = self.commander.clone();
                crate::utils::spawn_cleanup(async move {
                    let _ = commander.send(NeoReactorCommand::HangUp).await;
                    while set.join_next().await.is_some() {}
                    log::trace!("Dropped NeoReactor");
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn shutting_down_stops_the_cameras_then_the_reactor() -> AnyResult<()> {
        let config: Config = toml::from_str(
            "[[cameras]]\nname = \"cam\"\nusername = \"admin\"\naddress = \"127.0.0.1:1\"\npush_notifications = false\n",
        )?;
        let reactor = NeoReactor::new(config).await;
        reactor.get("cam").await?;

        // Well before a camera's time to stop runs out, since none is slow
        timeout(Duration::from_secs(2), reactor.shutdown()).await?;
        assert!(reactor.get("cam").await.is_err(), "the reactor still runs");
        Ok(())
    }
}
