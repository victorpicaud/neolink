use crate::{
    config::{Config, MqttServerConfig},
    AnyResult,
};
use anyhow::{anyhow, Context, Result};
use futures::future::FutureExt;
use log::*;
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event, EventLoop, Incoming, LastWill, MqttOptions, QoS,
    TlsConfiguration, Transport,
};
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::{
    sync::{
        broadcast::{channel as broadcast, Sender as BroadcastSender},
        mpsc::{channel as mpsc, Receiver as MpscReceiver, Sender as MpscSender},
        oneshot::{channel as oneshot, Sender as OneshotSender},
        watch::Receiver as WatchReceiver,
    },
    time::{sleep, Duration, Instant},
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    StreamExt,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) struct Mqtt {
    cancel: CancellationToken,
    outgoing_tx: MpscSender<MqttRequest>,
    set: JoinSet<Result<()>>,
}

impl Mqtt {
    pub(crate) async fn new(config: WatchReceiver<Config>) -> Self {
        let (incoming_tx, _) = broadcast::<MqttReply>(100);
        let (outgoing_tx, mut outgoing_rx) = mpsc::<MqttRequest>(100);
        let cancel = CancellationToken::new();
        let mut set = JoinSet::<AnyResult<()>>::new();

        // Thread that handles the mqttc side
        // including restarting it if the config changes
        let thread_cancel = cancel.clone();
        let mut thread_config = config;
        let thread_incoming_tx = incoming_tx;
        let thread_outgoing_tx = outgoing_tx.clone();
        set.spawn(async move {
            let mut mqtt_config = thread_config.borrow().mqtt.clone();
            let r = loop {
                break tokio::select! {
                    _ = thread_cancel.cancelled() => AnyResult::Ok(()),
                    v = thread_config.wait_for(|config| config.mqtt != mqtt_config).map(|res| res.map(|r| r.clone())) =>
                    {
                        mqtt_config = v?.mqtt.clone();
                        continue;
                    }
                    v = async {
                        let mut backend = MqttBackend {
                            incomming_tx: thread_incoming_tx.clone(),
                            outgoing_rx: &mut outgoing_rx,
                            outgoing_tx: thread_outgoing_tx.clone(),
                            config: mqtt_config.as_ref().unwrap(),
                            cancel: CancellationToken::new(),
                        };
                        backend.run().await
                    }, if mqtt_config.is_some() => {
                        if let Err(e) = &v {
                            log::error!("MQTT Client Connection Failed: {:?}", e);
                            sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                        v
                    },
                };
            };
            log::debug!("MQTT thread stopped: {:?}", r);
            r
        });

        Self {
            cancel,
            outgoing_tx,
            set,
            // Send the drop message on clean disconnect
        }
    }

    pub async fn subscribe<T: Into<String>>(&self, name: T) -> AnyResult<MqttInstance> {
        let (tx, rx) = oneshot();
        self.outgoing_tx
            .send(MqttRequest::Subscribe(name.into(), tx))
            .await?;
        rx.await?
    }
}

impl Drop for Mqtt {
    fn drop(&mut self) {
        log::trace!("Drop MQTT");
        let outgoing_tx = self.outgoing_tx.clone();
        let cancel = self.cancel.clone();
        let mut set = std::mem::take(&mut self.set);

        crate::utils::spawn_cleanup(async move {
            let (tx, rx) = oneshot();
            let _ = outgoing_tx.send(MqttRequest::HangUp(tx)).await;
            let _ = rx.await;

            log::debug!("Mqtt::drop Cancel");
            cancel.cancel();
            while set.join_next().await.is_some() {}
            log::trace!("Dropped MQTT");
        });
    }
}

struct MqttBackend<'a> {
    incomming_tx: BroadcastSender<MqttReply>,
    outgoing_rx: &'a mut MpscReceiver<MqttRequest>,
    outgoing_tx: MpscSender<MqttRequest>,
    config: &'a MqttServerConfig,
    cancel: CancellationToken,
}

impl<'a> MqttBackend<'a> {
    async fn run(&mut self) -> AnyResult<()> {
        log::trace!("Run MQTT Server");
        let mut mqttoptions = MqttOptions::new(
            format!("Neolink{}", Uuid::new_v4()),
            &self.config.broker_addr,
            self.config.port,
        );
        let max_size = 100 * (1024 * 1024);
        mqttoptions.set_max_packet_size(max_size, max_size);

        if let Some(transport) = tls_transport(self.config)? {
            mqttoptions.set_transport(transport);
        }

        if let Some((username, password)) = &self.config.credentials {
            mqttoptions.set_credentials(username, password);
        }

        mqttoptions.set_keep_alive(Duration::from_secs(5));

        // On unclean disconnect send this
        mqttoptions.set_last_will(LastWill::new(
            "neolink/status".to_string(),
            "offline",
            QoS::AtLeastOnce,
            true,
        ));

        let (client, mut connection) = AsyncClient::new(mqttoptions, 100);

        let client = Arc::new(client);
        let send_client = client.clone();
        send_client
            .publish(
                "neolink/status".to_string(),
                QoS::AtLeastOnce,
                true,
                "connected".to_string(),
            )
            .await?;
        log::debug!("MQTT Published Startup");
        let loop_cancel = CancellationToken::new();
        let _drop_guard = loop_cancel.clone().drop_guard();
        loop {
            let r = tokio::select! {
                v = self.outgoing_rx.recv() => {
                    let msg = v.ok_or(anyhow!("All outgoing MQTT channels closed"))?;

                    // Put it on a thread so that we don't block polling
                    let outgoing_tx = self.outgoing_tx.clone();
                    let incomming_tx = self.incomming_tx.clone();
                    let send_client = send_client.clone();
                    let cancel = self.cancel.clone();
                    let thread_cancel = loop_cancel.clone();
                    let server_config = self.config.clone();
                    tokio::task::spawn(async move {
                        tokio::select!{
                            _ = cancel.cancelled() => AnyResult::Ok(()),
                            _ = thread_cancel.cancelled() => AnyResult::Ok(()),
                            v = async {
                                match msg {
                                    MqttRequest::Send(msg, tx) =>  {
                                        let v = send_client.publish(
                                            msg.topic.clone(),
                                            QoS::AtLeastOnce,
                                            false,
                                            (*msg.message).clone(),
                                        ).await;
                                        match &v {
                                            Ok(()) => {
                                                let _ = tx.send(Ok(()));
                                            },
                                            Err(rumqttc::ClientError::Request(_)) | Err(rumqttc::ClientError::TryRequest(_)) => {
                                                // Requeue it
                                                outgoing_tx.send(MqttRequest::Send(msg, tx)).await?;
                                            }
                                        };
                                        v?;
                                    }
                                    MqttRequest::SendRetained(msg, tx) =>  {
                                        let v = send_client.publish(
                                            msg.topic.clone(),
                                            QoS::AtLeastOnce,
                                            true,
                                            (*msg.message).clone(),
                                        ).await;
                                        match &v {
                                            Ok(()) => {
                                                let _ = tx.send(Ok(()));
                                            },
                                            Err(rumqttc::ClientError::Request(_)) | Err(rumqttc::ClientError::TryRequest(_)) => {
                                                // Requeue it
                                                outgoing_tx.send(MqttRequest::Send(msg, tx)).await?;
                                            }
                                        };
                                        v?;
                                    }
                                    MqttRequest::HangUp(reply) => {
                                        send_client.publish(
                                            "neolink/status".to_string(),
                                            QoS::AtLeastOnce,
                                            true,
                                            "disconnected".to_string(),
                                        ).await?;
                                        let _ = reply.send(());
                                        return Err(anyhow!("Disconneting"));
                                    }
                                    MqttRequest::Subscribe(name, reply) => {
                                        let instance = MqttInstance {
                                            name,
                                            incomming_rx: BroadcastStream::new(incomming_tx.subscribe()),
                                            outgoing_tx: outgoing_tx.clone(),
                                            lag_warned_at: None,
                                        };
                                        let _ = reply.send(Ok(instance));
                                    },
                                    MqttRequest::LastWill{topic, message, reply} => {
                                        let last_will = LastWillMqtt::new(
                                            &server_config,
                                            topic,
                                            message,
                                        ).await;
                                        let _ = reply.send(last_will);
                                    }
                                }
                                AnyResult::Ok(())
                            } => v,
                        }
                    });

                    AnyResult::Ok(())
                },
                v = connection.poll() =>  {
                    let  notification = v.with_context(|| "MQTT connection dropped")?;
                    // Handle message on another thread so that we can keep polling
                    let client = client.clone();
                    let incomming_tx = self.incomming_tx.clone();
                    let cancel = self.cancel.clone();
                    let thread_cancel = loop_cancel.clone();
                    tokio::task::spawn(async move {
                        tokio::select!{
                            _ = cancel.cancelled() => AnyResult::Ok(()),
                            _ = thread_cancel.cancelled() => AnyResult::Ok(()),
                            v = async {
                                match notification {
                                    Event::Incoming(Incoming::ConnAck(connected)) => {
                                        if ConnectReturnCode::Success == connected.code {
                                            // Publish connected now that we are online
                                            client
                                            .publish(
                                                "neolink/status".to_string(),
                                                QoS::AtLeastOnce,
                                                true,
                                                "connected",
                                            )
                                            .await?;
                                            // We succesfully logged in. Now ask for the cameras subscription.
                                            client
                                            .subscribe("neolink/#".to_string(), QoS::AtMostOnce)
                                            .await?;
                                        }
                                    }
                                    Event::Incoming(Incoming::Publish(published_message)) => {
                                        if let Some(sub_topic) = published_message
                                            .topic
                                            .strip_prefix("neolink/")
                                        {
                                            let _ = incomming_tx
                                                .send(MqttReply {
                                                    topic: sub_topic.to_string(),
                                                    message: Arc::new(String::from_utf8_lossy(published_message.payload.as_ref())
                                                        .into_owned()),
                                                });
                                        }
                                    }
                                    _ => {}
                                }
                                AnyResult::Ok(())
                            } => v
                        }
                    });
                    AnyResult::Ok(())
                },
            };
            if r.is_ok() {
                continue;
            }
            break r;
        }?;
        Ok(())
    }
}

impl Drop for MqttBackend<'_> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub(crate) struct MqttInstance {
    outgoing_tx: MpscSender<MqttRequest>,
    incomming_rx: BroadcastStream<MqttReply>,
    name: String,
    /// When this reader last warned that it fell behind
    lag_warned_at: Option<Instant>,
}

/// A reader that keeps falling behind warns at most this often
const LAG_WARNING_INTERVAL: Duration = Duration::from_secs(60);

impl MqttInstance {
    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }

    pub async fn subscribe<T: Into<String>>(&self, name: T) -> AnyResult<Self> {
        let (tx, rx) = oneshot();
        self.outgoing_tx
            .send(MqttRequest::Subscribe(name.into(), tx))
            .await?;
        rx.await?
    }

    pub async fn resubscribe(&self) -> AnyResult<Self> {
        let (tx, rx) = oneshot();
        self.outgoing_tx
            .send(MqttRequest::Subscribe(self.name.clone(), tx))
            .await?;
        rx.await?
    }

    pub async fn send_message_with_root_topic(
        &self,
        root_topic: &str,
        sub_topic: &str,
        message: &str,
        retain: bool,
    ) -> AnyResult<()> {
        let topics = [
            root_topic.to_string(),
            self.name.clone(),
            sub_topic.to_string(),
        ]
        .iter()
        .filter(|s| !s.is_empty())
        .cloned()
        .collect::<Vec<_>>();
        if retain {
            let (tx, rx) = oneshot();
            self.outgoing_tx
                .send(MqttRequest::SendRetained(
                    MqttReply {
                        topic: topics.join("/"),
                        message: Arc::new(message.to_string()),
                    },
                    tx,
                ))
                .await?;
            rx.await??;
        } else {
            let (tx, rx) = oneshot();
            self.outgoing_tx
                .send(MqttRequest::Send(
                    MqttReply {
                        topic: topics.join("/"),
                        message: Arc::new(message.to_string()),
                    },
                    tx,
                ))
                .await?;
            rx.await??;
        }
        Ok(())
    }

    pub async fn send_message(
        &self,
        sub_topic: &str,
        message: &str,
        retain: bool,
    ) -> AnyResult<()> {
        self.send_message_with_root_topic("neolink", sub_topic, message, retain)
            .await?;
        Ok(())
    }

    pub(crate) async fn recv(&mut self) -> AnyResult<MqttReply> {
        Ok(loop {
            let mut msg = match self.incomming_rx.next().await {
                Some(Ok(msg)) => msg,
                // A burst of messages, such as the retained ones sent on
                // subscribing, can outrun a reader. Ending here used to stop
                // the reader for good.
                Some(Err(BroadcastStreamRecvError::Lagged(missed))) => {
                    let reader = if self.name.is_empty() {
                        "neolink"
                    } else {
                        &self.name
                    };
                    let message = format!(
                        "{reader}: {missed} MQTT messages arrived too fast to be read, skipped them"
                    );
                    if self
                        .lag_warned_at
                        .is_some_and(|at| at.elapsed() < LAG_WARNING_INTERVAL)
                    {
                        debug!("{message}");
                    } else {
                        warn!("{message}");
                        self.lag_warned_at = Some(Instant::now());
                    }
                    continue;
                }
                None => return Err(anyhow!("End of client data")),
            };
            // log::debug!("Got MQTT: {msg:?}");
            // log::debug!("self.name: {:?}", self.name);

            if self.name.is_empty() {
                break msg;
            } else {
                let mut topics = msg.topic.split('/');
                let sub_topic = topics.next();
                // log::debug!("topics: {:?}", msg.topic);
                // log::debug!("sub_topic: {sub_topic:?}");
                if sub_topic
                    .map(|subtopic| *subtopic == self.name)
                    .unwrap_or(false)
                {
                    msg.topic = topics.collect::<Vec<_>>().join("/");
                    // log::debug!("new topics: {:?}", msg.topic);
                    break msg;
                }
            }
        })
    }

    pub(crate) async fn last_will(&self, topic: &str, message: &str) -> AnyResult<LastWillMqtt> {
        let topic = if self.name.is_empty() {
            format!("neolink/{}", topic)
        } else {
            format!("neolink/{}/{}", self.name, topic)
        };

        let (tx, rx) = oneshot();
        self.outgoing_tx
            .send(MqttRequest::LastWill {
                topic,
                message: message.to_string(),
                reply: tx,
            })
            .await?;
        rx.await?
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MqttReply {
    pub(crate) topic: String,
    pub(crate) message: Arc<String>, // Messages can be long so avoid costly clones with an arc
}

impl MqttReply {
    pub(crate) fn as_ref(&self) -> MqttReplyRef<'_> {
        MqttReplyRef {
            topic: &self.topic,
            message: &self.message,
        }
    }
}

pub(crate) struct MqttReplyRef<'a> {
    pub(crate) topic: &'a str,
    pub(crate) message: &'a str,
}

enum MqttRequest {
    Send(MqttReply, OneshotSender<Result<()>>),
    SendRetained(MqttReply, OneshotSender<Result<()>>),
    HangUp(OneshotSender<()>),
    Subscribe(String, OneshotSender<Result<MqttInstance>>),
    LastWill {
        topic: String,
        message: String,
        reply: OneshotSender<Result<LastWillMqtt>>,
    },
}

/// The TLS to connect with, if the config asks for it. A CA or client
/// certificate that cannot be read is an error: the connection used to go
/// ahead without encryption, and send the credentials in the clear.
fn tls_transport(config: &MqttServerConfig) -> AnyResult<Option<Transport>> {
    let Some(ca_path) = &config.ca else {
        return Ok(None);
    };
    let ca = std::fs::read(ca_path)
        .with_context(|| format!("Could not read the MQTT ca {}", ca_path.display()))?;
    let client_auth = match &config.client_auth {
        Some((cert_path, key_path)) => Some((
            std::fs::read(cert_path).with_context(|| {
                format!(
                    "Could not read the MQTT client certificate {}",
                    cert_path.display()
                )
            })?,
            std::fs::read(key_path).with_context(|| {
                format!("Could not read the MQTT client key {}", key_path.display())
            })?,
        )),
        None => None,
    };
    Ok(Some(Transport::Tls(TlsConfiguration::Simple {
        ca,
        alpn: None,
        client_auth,
    })))
}

pub(crate) struct LastWillMqtt {
    cancel: CancellationToken,
}

impl LastWillMqtt {
    pub(crate) async fn new(
        config: &MqttServerConfig,
        topic: String,
        message: String,
    ) -> AnyResult<Self> {
        log::trace!("Run MQTT Last Will");
        let mut mqttoptions = MqttOptions::new(
            format!("NeolinkLastWill_{}_{}", topic, Uuid::new_v4()),
            &config.broker_addr,
            config.port,
        );
        let max_size = 100 * (1024 * 1024);
        mqttoptions.set_max_packet_size(max_size, max_size);

        if let Some(transport) = tls_transport(config)? {
            mqttoptions.set_transport(transport);
        }

        if let Some((username, password)) = &config.credentials {
            mqttoptions.set_credentials(username, password);
        }

        mqttoptions.set_keep_alive(Duration::from_secs(5));

        // On unclean disconnect send this
        mqttoptions.set_last_will(LastWill::new(topic, message, QoS::AtLeastOnce, true));

        let (client, connection) = AsyncClient::new(mqttoptions, 100);
        let cancel = CancellationToken::new();
        let thread_cancel = cancel.clone();

        tokio::task::spawn(async move {
            let r = poll_last_will(connection, thread_cancel).await;
            drop(client);
            r
        });

        Ok(LastWillMqtt { cancel })
    }
}

/// Keep the last-will connection alive until `cancel` fires or the
/// connection fails.
///
/// Cancellation must end the loop: `cancelled()` stays ready once the token
/// is cancelled, so treating it as just another iteration spins forever
/// without yielding and pins a runtime worker.
async fn poll_last_will(mut connection: EventLoop, cancel: CancellationToken) -> AnyResult<()> {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break Ok(()),
            v = connection.poll() => {
                v?;
            }
        }
    }
}

impl Drop for LastWillMqtt {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cancelling a last-will connection used to leave its task spinning at
    /// 100% CPU, so the loop never finished.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn last_will_loop_stops_on_cancel() {
        // A "broker" that accepts the TCP connection and never answers, so
        // `poll()` stays pending and only cancellation can end the loop.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut held = vec![];
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });

        let (_client, connection) =
            AsyncClient::new(MqttOptions::new("neolink-test", "127.0.0.1", port), 10);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(poll_last_will(connection, cancel.clone()));
        sleep(Duration::from_millis(200)).await;
        cancel.cancel();

        // Cancellation alone must end the task, promptly and cleanly.
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("last-will loop kept running after cancel");
        assert!(result.unwrap().is_ok());
    }

    #[test]
    fn a_ca_that_cannot_be_read_is_an_error_not_a_connection_in_the_clear() {
        let config: MqttServerConfig =
            toml::from_str("broker_addr = \"broker\"\nport = 8883\nca = \"/nonexistent/ca.pem\"\n")
                .unwrap();
        let Err(error) = tls_transport(&config) else {
            panic!("connected without the CA");
        };
        assert!(
            format!("{error:#}").contains("Could not read the MQTT ca"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn a_reader_that_fell_behind_skips_what_it_missed() -> AnyResult<()> {
        let (incoming_tx, incoming_rx) = broadcast(4);
        let mut instance = MqttInstance {
            outgoing_tx: mpsc(1).0,
            incomming_rx: BroadcastStream::new(incoming_rx),
            name: "cam".to_string(),
            lag_warned_at: None,
        };
        for i in 0..10 {
            incoming_tx.send(MqttReply {
                topic: format!("cam/topic{i}"),
                message: Arc::new(String::new()),
            })?;
        }

        // The first six were overwritten before they were read
        assert_eq!(instance.recv().await?.topic, "topic6");
        assert_eq!(instance.recv().await?.topic, "topic7");
        Ok(())
    }
}
