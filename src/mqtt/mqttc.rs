use crate::{
    common::Backoff,
    config::{Config, MqttServerConfig},
    AnyResult,
};
use anyhow::{anyhow, Context, Result};
use log::*;
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event, EventLoop, Incoming, LastWill, MqttOptions, Outgoing,
    QoS, TlsConfiguration, Transport,
};
use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    path::Path,
    sync::{Arc, OnceLock},
};
use tokio::task::JoinSet;
use tokio::{
    sync::{
        broadcast::{channel as broadcast, Sender as BroadcastSender},
        mpsc::{channel as mpsc, Receiver as MpscReceiver, Sender as MpscSender},
        oneshot::{channel as oneshot, Sender as OneshotSender},
        watch::{self, Receiver as WatchReceiver},
    },
    time::{sleep, timeout, Duration, Instant},
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    StreamExt,
};
use tokio_util::sync::CancellationToken;

/// Where neolink says whether it is connected
const STATUS_TOPIC: &str = "neolink/status";
const CONNECTED: &str = "connected";
/// What the broker says for neolink when it is gone, and what neolink says
/// when it leaves
const OFFLINE: &str = "offline";
/// What neolink listens to: its config, and the cameras' commands. All of
/// neolink/# brought back everything neolink publishes too, such as each
/// camera's preview every few seconds, only to be dropped.
const SUBSCRIPTIONS: [&str; 3] = ["neolink/config", "neolink/+/control/#", "neolink/+/query/#"];
/// How many requests the client queues while the broker is away. Senders wait
/// beyond that, rather than pile up previews.
const REQUEST_CAPACITY: usize = 100;

/// The client id this neolink uses, unless the config gives one
static CLIENT_ID: OnceLock<String> = OnceLock::new();

/// Sets the client id from what tells this neolink apart from another on the
/// same host: its config file and its RTSP port. The same on every
/// connection: a new one each time left the broker to publish the last will
/// of the old connection, "offline", after the new one said "connected".
pub(crate) fn set_client_id(config_path: &Path, config: &Config) {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|host| {
            host.trim()
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
                .take(32)
                .collect::<String>()
        })
        .filter(|host| !host.is_empty());
    let mut hasher = DefaultHasher::new();
    config_path
        .canonicalize()
        .unwrap_or_else(|_| config_path.to_path_buf())
        .hash(&mut hasher);
    config.bind_port.hash(&mut hasher);
    let hash = hasher.finish() as u32;
    let id = match host {
        Some(host) => format!("neolink-{host}-{hash:08x}"),
        None => format!("neolink-{hash:08x}"),
    };
    let _ = CLIENT_ID.set(id);
}

fn client_id(config: &MqttServerConfig) -> String {
    config.client_id.clone().unwrap_or_else(|| {
        CLIENT_ID
            .get()
            .cloned()
            .unwrap_or_else(|| "neolink".to_string())
    })
}

/// The options to connect with. A CA or client certificate that cannot be
/// read is an error: the connection used to go ahead without encryption, and
/// send the credentials in the clear.
fn mqtt_options(config: &MqttServerConfig, client_id: String) -> AnyResult<MqttOptions> {
    let mut options = MqttOptions::new(client_id, &config.broker_addr, config.port);
    let max_size = 100 * (1024 * 1024);
    options.set_max_packet_size(max_size, max_size);
    if let Some(ca_path) = &config.ca {
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
        options.set_transport(Transport::Tls(TlsConfiguration::Simple {
            ca,
            alpn: None,
            client_auth,
        }));
    }
    if let Some((username, password)) = &config.credentials {
        options.set_credentials(username, password);
    }
    options.set_keep_alive(Duration::from_secs(5));
    Ok(options)
}

pub(crate) struct Mqtt {
    cancel: CancellationToken,
    incoming_tx: BroadcastSender<MqttReply>,
    outgoing_tx: MpscSender<MqttRequest>,
    config: WatchReceiver<Config>,
    set: JoinSet<Result<()>>,
}

impl Mqtt {
    pub(crate) async fn new(config: WatchReceiver<Config>) -> Self {
        let (incoming_tx, _) = broadcast::<MqttReply>(100);
        let (outgoing_tx, mut outgoing_rx) = mpsc::<MqttRequest>(100);
        let cancel = CancellationToken::new();
        let mut set = JoinSet::<AnyResult<()>>::new();

        // Connects to the broker, and again if the config changes
        let thread_cancel = cancel.clone();
        let mut thread_config = config.clone();
        let thread_incoming_tx = incoming_tx.clone();
        set.spawn(async move {
            let mut mqtt_config = thread_config.borrow_and_update().mqtt.clone();
            loop {
                let result = tokio::select! {
                    _ = thread_cancel.cancelled() => return AnyResult::Ok(()),
                    v = thread_config.wait_for(|config| config.mqtt != mqtt_config) => {
                        mqtt_config = v?.mqtt.clone();
                        continue;
                    }
                    v = async {
                        match mqtt_config.as_ref() {
                            Some(server) => run_backend(server, &thread_incoming_tx, &mut outgoing_rx).await,
                            None => std::future::pending().await,
                        }
                    } => v,
                };
                match result {
                    // Hung up
                    Ok(()) => return Ok(()),
                    // It cannot work as configured: until the config changes
                    Err(e) => {
                        error!("MQTT: {e:#}");
                        tokio::select! {
                            _ = thread_cancel.cancelled() => return Ok(()),
                            v = thread_config.wait_for(|config| config.mqtt != mqtt_config) => {
                                mqtt_config = v?.mqtt.clone();
                            }
                        }
                    }
                }
            }
        });

        Self {
            cancel,
            incoming_tx,
            outgoing_tx,
            config,
            set,
        }
    }

    pub async fn subscribe<T: Into<String>>(&self, name: T) -> AnyResult<MqttInstance> {
        Ok(MqttInstance::new(
            name.into(),
            &self.incoming_tx,
            &self.outgoing_tx,
            &self.config,
        ))
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
            // Not for long: no one answers while the config cannot work
            let _ = timeout(Duration::from_secs(3), async {
                let _ = outgoing_tx.send(MqttRequest::HangUp(tx)).await;
                let _ = rx.await;
            })
            .await;

            log::debug!("Mqtt::drop Cancel");
            cancel.cancel();
            while set.join_next().await.is_some() {}
            log::trace!("Dropped MQTT");
        });
    }
}

/// Talks to the broker until told to hang up, reconnecting as long as it
/// takes. Fails only if the config cannot work.
async fn run_backend(
    server: &MqttServerConfig,
    incoming_tx: &BroadcastSender<MqttReply>,
    outgoing_rx: &mut MpscReceiver<MqttRequest>,
) -> AnyResult<()> {
    let mut options = mqtt_options(server, client_id(server))?;
    // On unclean disconnect send this
    options.set_last_will(LastWill::new(STATUS_TOPIC, OFFLINE, QoS::AtLeastOnce, true));
    let (client, eventloop) = AsyncClient::new(options, REQUEST_CAPACITY);
    let (connected_tx, connected_rx) = watch::channel(0u64);
    let closing = CancellationToken::new();

    let broker = format!("{}:{}", server.broker_addr, server.port);
    let mut event_loop = std::pin::pin!(poll_broker(
        eventloop,
        broker,
        incoming_tx.clone(),
        connected_tx,
        closing.clone()
    ));
    let hung_up = tokio::select! {
        v = &mut event_loop => return v,
        v = publish(&client, outgoing_rx, connected_rx) => v?,
    };
    // Let the event loop send the goodbye
    closing.cancel();
    let _ = timeout(Duration::from_secs(2), &mut event_loop).await;
    let _ = hung_up.send(());
    Ok(())
}

/// Polls the connection to the broker, which also reconnects it. Never waits
/// on the client, which waits on this to take its requests.
async fn poll_broker(
    mut eventloop: EventLoop,
    broker: String,
    incoming_tx: BroadcastSender<MqttReply>,
    connected: watch::Sender<u64>,
    closing: CancellationToken,
) -> AnyResult<()> {
    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                if ack.code == ConnectReturnCode::Success {
                    info!("MQTT: Connected to {broker}");
                    backoff.reset();
                    connected.send_modify(|connects| *connects += 1);
                } else {
                    warn!("MQTT: {broker} refused the connection: {:?}", ack.code);
                }
            }
            Ok(Event::Incoming(Incoming::Publish(published))) => {
                if let Some(topic) = published.topic.strip_prefix("neolink/") {
                    let _ = incoming_tx.send(MqttReply {
                        topic: topic.to_string(),
                        message: Arc::new(String::from_utf8_lossy(&published.payload).into_owned()),
                        retain: published.retain,
                    });
                }
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) if closing.is_cancelled() => return Ok(()),
            Ok(_) => {}
            Err(_) if closing.is_cancelled() => return Ok(()),
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("MQTT: Lost the connection to {broker}, trying again in {delay:.1?}: {e}");
                sleep(delay).await;
            }
        }
    }
}

/// Sends the requests in the order they come: the publishes used to go out
/// each on a task of its own, in any order. On every connection, subscribes
/// again and republishes the state, as the broker forgets the subscriptions,
/// and may have lost the retained messages. Returns where to answer a hang up.
async fn publish(
    client: &AsyncClient,
    requests: &mut MpscReceiver<MqttRequest>,
    mut connected: watch::Receiver<u64>,
) -> AnyResult<OneshotSender<()>> {
    // The latest retained message on every topic
    let mut retained: BTreeMap<String, Arc<String>> = BTreeMap::new();
    loop {
        tokio::select! {
            biased;
            v = connected.changed() => {
                v?;
                for topic in SUBSCRIPTIONS {
                    client.subscribe(topic, QoS::AtMostOnce).await?;
                }
                client.publish(STATUS_TOPIC, QoS::AtLeastOnce, true, CONNECTED).await?;
                for (topic, message) in retained.iter() {
                    client
                        .publish(topic, QoS::AtLeastOnce, true, message.as_bytes().to_vec())
                        .await?;
                }
            }
            request = requests.recv() => match request.ok_or(anyhow!("All outgoing MQTT channels closed"))? {
                MqttRequest::Publish(msg, reply) => {
                    if msg.retain {
                        retained.insert(msg.topic.clone(), msg.message.clone());
                    }
                    // Retained or not as asked: a retained message used to be
                    // sent again unretained when it had to wait
                    let sent = client
                        .publish(&msg.topic, QoS::AtLeastOnce, msg.retain, msg.message.as_bytes().to_vec())
                        .await;
                    let _ = reply.send(sent.map_err(Into::into));
                }
                MqttRequest::Republish(topic) => {
                    if let Some(message) = retained.get(&topic) {
                        client
                            .publish(&topic, QoS::AtLeastOnce, true, message.as_bytes().to_vec())
                            .await?;
                    }
                }
                MqttRequest::HangUp(reply) => {
                    client.publish(STATUS_TOPIC, QoS::AtLeastOnce, true, OFFLINE).await?;
                    client.disconnect().await?;
                    return Ok(reply);
                }
            }
        }
    }
}

pub(crate) struct MqttInstance {
    outgoing_tx: MpscSender<MqttRequest>,
    incoming_tx: BroadcastSender<MqttReply>,
    incomming_rx: BroadcastStream<MqttReply>,
    config: WatchReceiver<Config>,
    name: String,
    /// When this reader last warned that it fell behind
    lag_warned_at: Option<Instant>,
}

/// A reader that keeps falling behind warns at most this often
const LAG_WARNING_INTERVAL: Duration = Duration::from_secs(60);

impl MqttInstance {
    fn new(
        name: String,
        incoming_tx: &BroadcastSender<MqttReply>,
        outgoing_tx: &MpscSender<MqttRequest>,
        config: &WatchReceiver<Config>,
    ) -> Self {
        Self {
            outgoing_tx: outgoing_tx.clone(),
            incoming_tx: incoming_tx.clone(),
            incomming_rx: BroadcastStream::new(incoming_tx.subscribe()),
            config: config.clone(),
            name,
            lag_warned_at: None,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }

    pub async fn subscribe<T: Into<String>>(&self, name: T) -> AnyResult<Self> {
        Ok(Self::new(
            name.into(),
            &self.incoming_tx,
            &self.outgoing_tx,
            &self.config,
        ))
    }

    pub async fn resubscribe(&self) -> AnyResult<Self> {
        self.subscribe(self.name.clone()).await
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
        let msg = MqttReply {
            topic: topics.join("/"),
            message: Arc::new(message.to_string()),
            retain,
        };
        let (tx, rx) = oneshot();
        self.outgoing_tx.send(MqttRequest::Publish(msg, tx)).await?;
        rx.await??;
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

            if self.name.is_empty() {
                break msg;
            } else {
                let mut topics = msg.topic.split('/');
                let sub_topic = topics.next();
                if sub_topic
                    .map(|subtopic| *subtopic == self.name)
                    .unwrap_or(false)
                {
                    msg.topic = topics.collect::<Vec<_>>().join("/");
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
        Ok(LastWillMqtt::new(
            self.config.clone(),
            topic,
            message.to_string(),
            self.outgoing_tx.clone(),
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MqttReply {
    pub(crate) topic: String,
    pub(crate) message: Arc<String>, // Messages can be long so avoid costly clones with an arc
    /// Whether the broker kept it from before: it then comes on subscribing,
    /// whenever it was sent
    pub(crate) retain: bool,
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
    Publish(MqttReply, OneshotSender<Result<()>>),
    /// Publish a topic's retained message again, if there is one
    Republish(String),
    HangUp(OneshotSender<()>),
}

/// A connection whose only job is its last will, which the broker publishes
/// once the connection is gone, as when neolink stops without a word
pub(crate) struct LastWillMqtt {
    cancel: CancellationToken,
}

impl LastWillMqtt {
    /// Reconnects as long as it takes, to whichever broker the config names.
    /// It used to give up at the first error, and the broker then had no last
    /// will to publish.
    fn new(
        config: WatchReceiver<Config>,
        topic: String,
        message: String,
        outgoing_tx: MpscSender<MqttRequest>,
    ) -> Self {
        let cancel = CancellationToken::new();
        tokio::task::spawn(keep_last_will(
            config,
            topic,
            message,
            outgoing_tx,
            cancel.clone(),
        ));
        LastWillMqtt { cancel }
    }
}

async fn keep_last_will(
    mut config: WatchReceiver<Config>,
    topic: String,
    message: String,
    outgoing_tx: MpscSender<MqttRequest>,
    cancel: CancellationToken,
) {
    loop {
        let server = config.borrow_and_update().mqtt.clone();
        let connection = async {
            let Some(server) = server.as_ref() else {
                return std::future::pending().await;
            };
            let mut hasher = DefaultHasher::new();
            topic.hash(&mut hasher);
            let id = format!("{}-will-{:08x}", client_id(server), hasher.finish() as u32);
            let mut options = match mqtt_options(server, id) {
                Ok(options) => options,
                // The main connection says why
                Err(_) => return std::future::pending().await,
            };
            options.set_last_will(LastWill::new(
                &topic,
                message.as_str(),
                QoS::AtLeastOnce,
                true,
            ));
            let (client, connection) = AsyncClient::new(options, 10);
            let polled = poll_last_will(
                connection,
                topic.clone(),
                outgoing_tx.clone(),
                cancel.clone(),
            )
            .await;
            drop(client);
            polled
        };
        tokio::select! {
            _ = cancel.cancelled() => return,
            v = config.wait_for(|config| config.mqtt != server) => if v.is_err() {
                return;
            },
            _ = connection => return,
        }
    }
}

/// Keeps the last-will connection up until `cancel` fires, reconnecting
/// after an error.
///
/// Cancellation must end the loop: `cancelled()` stays ready once the token
/// is cancelled, so treating it as just another iteration spins forever
/// without yielding and pins a runtime worker.
async fn poll_last_will(
    mut connection: EventLoop,
    topic: String,
    outgoing_tx: MpscSender<MqttRequest>,
    cancel: CancellationToken,
) -> AnyResult<()> {
    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break Ok(()),
            v = connection.poll() => match v {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    backoff.reset();
                    // Connecting again took over the connection from before,
                    // and the broker published that one's will then: what the
                    // topic really says goes after it. Skipped rather than
                    // waited for if the publisher is busy, so that this
                    // connection keeps up with its broker.
                    let _ = outgoing_tx.try_send(MqttRequest::Republish(topic.clone()));
                }
                Ok(_) => {}
                Err(e) => {
                    let delay = backoff.next_delay();
                    debug!("MQTT: A last will connection was lost, trying again in {delay:.1?}: {e}");
                    tokio::select! {
                        _ = cancel.cancelled() => break Ok(()),
                        _ = sleep(delay) => {}
                    }
                }
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
    use rumqttc::Request;

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
        let task = tokio::spawn(poll_last_will(
            connection,
            "neolink/cam/status".to_string(),
            mpsc(1).0,
            cancel.clone(),
        ));
        sleep(Duration::from_millis(200)).await;
        cancel.cancel();

        // Cancellation alone must end the task, promptly and cleanly.
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("last-will loop kept running after cancel");
        assert!(result.unwrap().is_ok());
    }

    fn config() -> WatchReceiver<Config> {
        watch::channel(toml::from_str::<Config>("cameras = []").unwrap()).1
    }

    #[tokio::test]
    async fn a_reader_that_fell_behind_skips_what_it_missed() -> AnyResult<()> {
        let (incoming_tx, _) = broadcast(4);
        let mut instance =
            MqttInstance::new("cam".to_string(), &incoming_tx, &mpsc(1).0, &config());
        for i in 0..10 {
            incoming_tx.send(MqttReply {
                topic: format!("cam/topic{i}"),
                message: Arc::new(String::new()),
                retain: false,
            })?;
        }

        // The first six were overwritten before they were read
        assert_eq!(instance.recv().await?.topic, "topic6");
        assert_eq!(instance.recv().await?.topic, "topic7");
        Ok(())
    }

    /// What was asked of the broker: the topics subscribed to, and those
    /// published to, with whether they were retained
    fn asked(requests: &flume::Receiver<Request>) -> (Vec<String>, Vec<(String, bool)>) {
        let mut subscribed = vec![];
        let mut published = vec![];
        for request in requests.drain() {
            match request {
                Request::Subscribe(subscribe) => {
                    subscribed.extend(subscribe.filters.into_iter().map(|filter| filter.path))
                }
                Request::Publish(publish) => published.push((publish.topic, publish.retain)),
                _ => {}
            }
        }
        (subscribed, published)
    }

    async fn send(outgoing_tx: &MpscSender<MqttRequest>, topic: &str, retain: bool) {
        let (tx, rx) = oneshot();
        let msg = MqttReply {
            topic: topic.to_string(),
            message: Arc::new("message".to_string()),
            retain,
        };
        outgoing_tx
            .send(MqttRequest::Publish(msg, tx))
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn every_connection_gets_the_subscriptions_and_the_state_again() {
        let (request_tx, requests) = flume::bounded(100);
        let client = AsyncClient::from_senders(request_tx);
        let (outgoing_tx, mut outgoing_rx) = mpsc(10);
        let (connected_tx, connected_rx) = watch::channel(0u64);
        let publisher = tokio::spawn(async move {
            publish(&client, &mut outgoing_rx, connected_rx)
                .await
                .map(|_| ())
        });

        send(&outgoing_tx, "neolink/cam/status", true).await;
        send(&outgoing_tx, "neolink/cam/control/led", false).await;
        send(&outgoing_tx, "neolink/cam/status", true).await;
        let (subscribed, published) = asked(&requests);
        assert!(subscribed.is_empty());
        let topic = |topic: &str, retain| (topic.to_string(), retain);
        assert_eq!(
            published,
            [
                topic("neolink/cam/status", true),
                topic("neolink/cam/control/led", false),
                topic("neolink/cam/status", true),
            ],
            "in the order they came"
        );

        // (Re)connected
        connected_tx.send_modify(|connects| *connects += 1);
        let (mut subscribed, mut published) = (vec![], vec![]);
        for _ in 0..100 {
            let (more_subscribed, more_published) = asked(&requests);
            subscribed.extend(more_subscribed);
            published.extend(more_published);
            if published.len() >= 2 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(subscribed, SUBSCRIPTIONS);
        assert_eq!(
            published,
            [
                topic("neolink/status", true),
                topic("neolink/cam/status", true)
            ],
            "neolink is connected, and the state is as it was"
        );
        publisher.abort();
    }

    #[tokio::test]
    async fn a_topic_is_published_again_after_a_last_will_on_it() {
        let (request_tx, requests) = flume::bounded(100);
        let client = AsyncClient::from_senders(request_tx);
        let (outgoing_tx, mut outgoing_rx) = mpsc(10);
        let (_connected_tx, connected_rx) = watch::channel(0u64);
        let publisher = tokio::spawn(async move {
            publish(&client, &mut outgoing_rx, connected_rx)
                .await
                .map(|_| ())
        });
        send(&outgoing_tx, "neolink/cam/status", true).await;
        asked(&requests);

        for topic in ["neolink/cam/status", "neolink/cam/never/sent"] {
            outgoing_tx
                .send(MqttRequest::Republish(topic.to_string()))
                .await
                .unwrap();
        }
        send(&outgoing_tx, "neolink/cam/after", false).await;
        let (_, published) = asked(&requests);
        let topic = |topic: &str, retain| (topic.to_string(), retain);
        assert_eq!(
            published,
            [
                topic("neolink/cam/status", true),
                topic("neolink/cam/after", false)
            ]
        );
        publisher.abort();
    }

    #[test]
    fn a_ca_that_cannot_be_read_is_an_error_not_a_connection_in_the_clear() {
        let config: MqttServerConfig =
            toml::from_str("broker_addr = \"broker\"\nport = 8883\nca = \"/nonexistent/ca.pem\"\n")
                .unwrap();
        let error = mqtt_options(&config, "neolink-test".to_string()).unwrap_err();
        assert!(
            format!("{error:#}").contains("Could not read the MQTT ca"),
            "{error:#}"
        );
    }
}
