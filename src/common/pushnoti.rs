//! This thread handles push notifications
//! the last notification is pushed into a watcher
//! as is, which comes fromt the json structure
//!

use anyhow::Context;
use fcm_push_listener::*;
use std::collections::{HashMap, VecDeque};
use std::{fs, path::PathBuf, sync::Arc};
use tokio::{
    sync::{
        mpsc::{Receiver as MpscReceiver, Sender as MpscSender},
        oneshot::Sender as OneshotSender,
        watch::{channel as watch, Receiver as WatchReceiver, Sender as WatchSender},
        Mutex, RwLock,
    },
    time::{sleep, timeout, Duration},
};

use super::NeoInstance;
use crate::{config::Config, AnyResult};

/// How many delivered message ids to remember. They are only passed back to
/// FCM on reconnect so the same messages are not delivered twice.
const MAX_RECEIVED_IDS: usize = 1000;
/// Delay before retrying a failed registration; doubles up to `MAX_RETRY`
const MIN_RETRY: Duration = Duration::from_secs(3);
/// Longest delay between registration attempts
const MAX_RETRY: Duration = Duration::from_secs(60 * 60);

/// What the cameras need to know to send us push notifications
#[derive(Clone)]
struct PnRegistration {
    fcm_token: String,
    uid: String,
}

pub(crate) struct PushNotiThread {
    pn_watcher: Arc<WatchSender<Option<PushNoti>>>,
    /// Cameras that asked for push notifications, by name
    registered_cameras: Arc<Mutex<HashMap<String, NeoInstance>>>,
    received_ids: Arc<RwLock<VecDeque<String>>>,
    registration: WatchSender<Option<PnRegistration>>,
}

// The push notification
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct PushNoti {
    pub(crate) message: String,
    pub(crate) id: Option<String>,
}

pub(crate) enum PnRequest {
    Get {
        sender: OneshotSender<WatchReceiver<Option<PushNoti>>>,
    },
    Activate {
        instance: NeoInstance,
        sender: OneshotSender<AnyResult<()>>,
    },
    AddPushID {
        id: String,
    },
}

impl PushNotiThread {
    pub(crate) async fn new() -> AnyResult<Self> {
        let (pn_watcher, _) = watch(None);
        let (registration, _) = watch(None);

        Ok(PushNotiThread {
            pn_watcher: Arc::new(pn_watcher),
            registered_cameras: Default::default(),
            received_ids: Default::default(),
            registration,
        })
    }

    /// Runs until the request channel closes.
    ///
    /// Requests are always answered, whether or not push notifications are
    /// enabled or registered yet: cameras forward requests here and must never
    /// be left waiting on this thread.
    pub(crate) async fn run(
        &self,
        mut config: WatchReceiver<Config>,
        sender: MpscSender<PnRequest>,
        requests: &mut MpscReceiver<PnRequest>,
    ) -> AnyResult<()> {
        tokio::select! {
            v = self.serve_requests(requests) => v,
            v = async {
                loop {
                    // Wait until push notifications are enabled
                    config.wait_for(|c| c.cameras.iter().any(|cam| cam.push_notifications)).await?;
                    tokio::select! {
                        v = self.listen(&sender) => {
                            log::debug!("Push notification listener ended: {v:?}");
                            sleep(MIN_RETRY).await;
                        }
                        // Stop listening once they are disabled
                        v = async {
                            config
                                .wait_for(|c| c.cameras.iter().all(|cam| !cam.push_notifications))
                                .await
                                .map(|_| ())
                        } => {
                            v?;
                        }
                    }
                }
            } => v,
        }
    }

    async fn serve_requests(&self, requests: &mut MpscReceiver<PnRequest>) -> AnyResult<()> {
        while let Some(msg) = requests.recv().await {
            match msg {
                PnRequest::Get { sender } => {
                    let _ = sender.send(self.pn_watcher.subscribe());
                }
                PnRequest::Activate { instance, sender } => {
                    // Talking to the camera can take a while, so it happens off
                    // this loop. Without a registration yet the camera is
                    // registered by `listen` once one is available.
                    let registered_cameras = self.registered_cameras.clone();
                    let registration = self.registration.borrow().clone();
                    tokio::task::spawn(async move {
                        let r = async {
                            let name = instance.config().await?.borrow().name.clone();
                            registered_cameras
                                .lock()
                                .await
                                .insert(name, instance.clone());
                            match registration {
                                Some(registration) => {
                                    register_camera(&instance, &registration).await
                                }
                                None => Ok(()),
                            }
                        }
                        .await;
                        let _ = sender.send(r);
                    });
                }
                PnRequest::AddPushID { id } => {
                    log::trace!("Recived Push Notifcation of ID: {id}");
                    let mut ids = self.received_ids.write().await;
                    if !ids.contains(&id) {
                        ids.push_back(id);
                        while ids.len() > MAX_RECEIVED_IDS {
                            ids.pop_front();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Register with FCM (retrying with a capped exponential backoff) and
    /// listen for push messages
    async fn listen(&self, sender: &MpscSender<PnRequest>) -> AnyResult<()> {
        let token_path = token_path();
        log::debug!("Push notification details are saved to {:?}", token_path);
        let mut retry = MIN_RETRY;

        loop {
            let registration = match load_or_register(token_path.as_ref()).await {
                Ok(registration) => registration,
                Err(e) => {
                    log::warn!(
                        "Issue connecting to push notifications server, retrying in {:?}: {:?}",
                        retry,
                        e
                    );
                    sleep(retry).await;
                    retry = (retry * 2).min(MAX_RETRY);
                    continue;
                }
            };

            // Send registration.fcm_token to the server to allow it to send push messages to you.
            log::debug!("registration.fcm_token: {}", registration.fcm_token);
            let md5ed = md5::compute(format!("WHY_REOLINK_{:?}", registration.fcm_token));
            let pn_registration = PnRegistration {
                fcm_token: registration.fcm_token.clone(),
                uid: format!("{:X}", md5ed),
            };
            log::debug!("push notification UID: {}", pn_registration.uid);
            self.registration
                .send_replace(Some(pn_registration.clone()));

            for instance in self.registered_cameras.lock().await.values() {
                let instance = instance.clone();
                let pn_registration = pn_registration.clone();
                tokio::task::spawn(async move {
                    let _ = register_camera(&instance, &pn_registration).await;
                });
            }

            log::debug!("Push notification Listening");
            let thread_pn_watcher = self.pn_watcher.clone();
            let failed = loop {
                let mut listener = FcmPushListener::create(
                    registration.clone(),
                    |message: FcmMessage| {
                        log::debug!("Got FCM Message: {:?}", message.payload_json);
                        if let Some(id) = message.persistent_id.clone() {
                            // Don't worry if queue is full we will just not register as received yet
                            let _ = sender.try_send(PnRequest::AddPushID { id });
                        }
                        thread_pn_watcher.send_replace(Some(PushNoti {
                            message: message.payload_json,
                            id: message.persistent_id,
                        }));
                    },
                    self.received_ids.read().await.iter().cloned().collect(),
                );
                match timeout(Duration::from_secs(60 * 5), listener.connect()).await {
                    Ok(Ok(_)) => {
                        log::debug!("Push notification listener reported normal shutdown");
                        break false;
                    }
                    Ok(Err(e)) => {
                        use fcm_push_listener::Error::*;
                        match &e {
                            MissingMessagePayload
                            | MissingCryptoMetadata
                            | ProtobufDecode(_)
                            | Base64Decode(_) => {
                                // Wipe data so next call is a new token
                                if let Some(token_path) = token_path.as_ref() {
                                    let _ = fs::write(token_path, "");
                                }
                                log::debug!(
                                    "Error on push notification listener: {:?}. Clearing token",
                                    e
                                );
                            }
                            _ => {
                                log::debug!("Error on push notification listener: {:?}", e);
                            }
                        }
                        break true;
                    }
                    Err(_) => {
                        // Reconnect periodically
                        continue;
                    }
                }
            };

            if failed {
                sleep(retry).await;
                retry = (retry * 2).min(MAX_RETRY);
            } else {
                retry = MIN_RETRY;
                sleep(retry).await;
            }
        }
    }
}

/// Where the FCM registration is saved, if the config directory is usable
fn token_path() -> Option<PathBuf> {
    let mut dir = dirs::config_dir()?;
    dir.push("neolink");
    if let Err(e) = fs::create_dir_all(&dir) {
        log::warn!(
            "Push notification details will not be saved: cannot create {:?}: {}",
            dir,
            e
        );
        return None;
    }
    dir.push("neolink_token.toml");
    Some(dir)
}

/// Load the saved FCM registration, or register a new one and save it
async fn load_or_register(token_path: Option<&PathBuf>) -> AnyResult<Registration> {
    if let Some(Ok(Ok(registration))) = token_path.map(|token_path| {
        fs::read_to_string(token_path).map(|v| toml::from_str::<Registration>(&v))
    }) {
        log::debug!("Loaded push notification token");
        return Ok(registration);
    }

    log::debug!("Registering new push notification token");
    let sender_id = "743639030586"; // andriod
                                    // let sender_id = "696841269229"; // ios

    // let firebase_app_id = "1:743639030586:android:86f60a4fb7143876";
    // let firebase_project_id = "reolink-login";
    // let firebase_api_key = "AIzaSyBEUIuWHnnOEwFahxWgQB4Yt4NsgOmkPyE";
    // let vapid_key = "????";
    let registration = fcm_push_listener::register(sender_id).await?;
    let new_token =
        toml::to_string(&registration).with_context(|| "Unable to serialise fcm token")?;
    if let Some(Err(e)) = token_path.map(|token_path| fs::write(token_path, &new_token)) {
        log::warn!(
            "Unable to save push notification details to {:#?} because of the error {:#?}",
            token_path,
            e
        );
    }
    Ok(registration)
}

/// Tell a camera where to send its push notifications
async fn register_camera(instance: &NeoInstance, registration: &PnRegistration) -> AnyResult<()> {
    let fcm_token = registration.fcm_token.clone();
    let uid = registration.uid.clone();
    instance
        .run_task(|camera| {
            let fcm_token = fcm_token.clone();
            let uid = uid.clone();
            Box::pin(async move {
                let r = camera.send_pushinfo_android(&fcm_token, &uid).await;
                log::debug!(
                    "Registered {} for push notifications: {:?}",
                    camera.uid().await?,
                    r
                );
                r?;
                AnyResult::Ok(())
            })
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};

    /// Cameras forward their requests here and wait for the reply, so they
    /// must be answered even when no FCM registration exists (it never does
    /// once Google rejects the registration).
    #[tokio::test]
    async fn get_requests_are_answered_without_a_registration() {
        let pn = PushNotiThread::new().await.unwrap();
        let (tx, mut rx) = mpsc::channel(10);
        let client = async {
            // More requests than the channel holds
            for _ in 0..200 {
                let (sender, reply) = oneshot::channel();
                tx.send(PnRequest::Get { sender }).await.unwrap();
                tokio::time::timeout(Duration::from_secs(1), reply)
                    .await
                    .expect("push notification request was not answered")
                    .unwrap();
            }
        };
        tokio::select! {
            _ = pn.serve_requests(&mut rx) => panic!("request server stopped"),
            _ = client => {}
        }
    }

    #[tokio::test]
    async fn received_ids_are_bounded() {
        let pn = PushNotiThread::new().await.unwrap();
        let (tx, mut rx) = mpsc::channel(100);
        let client = async move {
            for i in 0..MAX_RECEIVED_IDS + 50 {
                tx.send(PnRequest::AddPushID { id: i.to_string() })
                    .await
                    .unwrap();
            }
            // A repeat is not stored twice
            tx.send(PnRequest::AddPushID {
                id: (MAX_RECEIVED_IDS + 49).to_string(),
            })
            .await
            .unwrap();
            // Dropping `tx` here ends `serve_requests`
        };
        let (served, _) = tokio::join!(pn.serve_requests(&mut rx), client);
        served.unwrap();

        let ids = pn.received_ids.read().await;
        assert_eq!(ids.len(), MAX_RECEIVED_IDS);
        assert_eq!(ids.front().map(String::as_str), Some("50"));
        let newest = (MAX_RECEIVED_IDS + 49).to_string();
        assert_eq!(ids.back(), Some(&newest));
    }
}
