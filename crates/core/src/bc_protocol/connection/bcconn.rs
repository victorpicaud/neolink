use super::BcSubscription;
use crate::{bc::model::*, Error, Result};
use futures::future::BoxFuture;
use futures::sink::{Sink, SinkExt};
use futures::stream::{Stream, StreamExt};
use log::*;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tokio::{sync::RwLock, task::JoinSet};

type MsgHandler = dyn 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>;

/// How many messages a request/reply subscription holds before it overflows
const REPLY_CAPACITY: usize = 100;
/// How many camera-initiated messages wait for their handler
const HANDLER_CAPACITY: usize = 20;
/// How long `shutdown` waits for what is queued, like a logout, to go out
const FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

/// What a subscription does when its subscriber falls behind and its queue is
/// full
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Overflow {
    /// End the subscription with [`Error::SubscriptionOverflow`]: for replies,
    /// and for streams where a gap would corrupt whatever comes after it
    Close,
    /// Drop the new message: for events that a later one supersedes
    DropNewest,
}

/// Where the messages for one subscriber go
struct Route {
    tx: Sender<Result<Bc>>,
    overflow: Overflow,
    /// Tells routes apart, so that a handler's guard removes its own route
    /// and never a newer one for the same message
    id: u64,
}

impl Route {
    /// Hands a message over without ever waiting, so that a subscriber that
    /// fell behind cannot hold up the messages for every other one, pings
    /// included. Returns false once the route is gone.
    fn deliver(&self, bc: Bc) -> bool {
        let msg_id = bc.meta.msg_id;
        // The last slot is kept for the error that ends the subscription
        if self.tx.capacity() > 1 {
            return self.tx.try_send(Ok(bc)).is_ok();
        }
        match self.overflow {
            Overflow::DropNewest => {
                debug!("A handler of message {msg_id} is behind, dropping a message");
                !self.tx.is_closed()
            }
            Overflow::Close => {
                warn!("A subscriber to message {msg_id} fell behind, ending its subscription");
                self.fail(Error::SubscriptionOverflow { msg_id });
                false
            }
        }
    }

    /// Ends the subscription with an error, in the slot kept for it
    fn fail(&self, error: Error) {
        let _ = self.tx.try_send(Err(error));
    }
}

#[derive(Default)]
struct Routes {
    /// Replies and streams, by message ID then number. A route without a
    /// number yet takes the number of the first message it gets.
    num: BTreeMap<u32, BTreeMap<Option<u16>, Route>>,
    /// Messages the camera starts, by message ID whatever their number: to a
    /// handler, or to an event subscription
    id: BTreeMap<u32, Route>,
}

impl Routes {
    /// Forgets the routes whose subscriber is gone
    fn prune(&mut self) {
        for routes in self.num.values_mut() {
            routes.retain(|_, route| !route.tx.is_closed());
        }
        self.num.retain(|_, routes| !routes.is_empty());
        self.id.retain(|_, route| !route.tx.is_closed());
    }

    fn deliver(&mut self, bc: Bc) {
        let msg_id = bc.meta.msg_id;
        let msg_num = bc.meta.msg_num;
        if let Some(route) = self.id.get(&msg_id) {
            if !route.deliver(bc) {
                self.id.remove(&msg_id);
            }
            return;
        }
        let Some(routes) = self.num.get_mut(&msg_id) else {
            trace!("Ignoring uninteresting message id {msg_id} (number: {msg_num})");
            return;
        };
        // A route waiting for its number takes this one
        if !routes.contains_key(&Some(msg_num)) {
            if let Some(route) = routes.remove(&None) {
                routes.insert(Some(msg_num), route);
            }
        }
        match routes.get(&Some(msg_num)) {
            Some(route) => {
                if !route.deliver(bc) {
                    routes.remove(&Some(msg_num));
                }
            }
            None => trace!("Ignoring uninteresting message id {msg_id} (number: {msg_num})"),
        }
    }

    /// Tells every subscriber, without waiting on any of them
    fn fail_all(&mut self, error: &Error) {
        for route in self
            .num
            .values()
            .flat_map(|routes| routes.values())
            .chain(self.id.values())
        {
            route.fail(error.clone());
        }
        self.num.clear();
        self.id.clear();
    }
}

/// What the writer task sends to the camera
enum Outgoing {
    Bc(Box<Bc>),
    /// Answered once everything queued before it has been written
    Flush(oneshot::Sender<()>),
}

pub(crate) type BcConnSink = Box<dyn Sink<Bc, Error = Error> + Send + Sync + Unpin>;
pub(crate) type BcConnSource = Box<dyn Stream<Item = Result<Bc>> + Send + Sync + Unpin>;

/// A shareable connection to a camera.  Handles serialization of messages.  To send/receive, call
/// .[subscribe()] with a message number.  You can use the BcSubscription to send or receive only
/// messages with that number; each incoming message is routed to its appropriate subscriber.
///
/// There can be only one subscriber per kind of message at a time.
pub struct BcConnection {
    sink: Sender<Outgoing>,
    poll_commander: Sender<PollCommand>,
    rx_thread: RwLock<JoinSet<Result<()>>>,
    cancel: CancellationToken,
    /// Set by `shutdown`: nothing new may be sent from then on
    closing: AtomicBool,
    next_route_id: AtomicU64,
}

impl BcConnection {
    pub async fn new(mut sink: BcConnSink, mut source: BcConnSource) -> Result<BcConnection> {
        let (sinker, mut sinker_rx) = channel::<Outgoing>(100);
        let cancel = CancellationToken::new();

        let (poll_commander, poll_commanded) = channel(200);
        let mut poller = Poller {
            routes: Default::default(),
            reciever: ReceiverStream::new(poll_commanded),
        };

        let mut rx_thread = JoinSet::<Result<()>>::new();
        let thread_poll_commander = poll_commander.clone();
        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => {
                    Result::Ok(())
                },
                v = async {
                    let sender = thread_poll_commander;
                    while let Some(bc) = source.next().await {
                        sender.send(PollCommand::Bc(Box::new(bc))).await?;
                    }
                    // The camera closed the connection. Ending quietly here
                    // left every request waiting forever (#229).
                    let _ = sender
                        .send(PollCommand::Bc(Box::new(Err(Error::DroppedConnection))))
                        .await;
                    Err(Error::DroppedConnection)
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    while let Some(outgoing) = sinker_rx.recv().await {
                        match outgoing {
                            Outgoing::Bc(bc) => sink.send(*bc).await?,
                            Outgoing::Flush(done) => {
                                let _ = done.send(());
                            }
                        }
                    }
                    Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            // `run` only returns once polling is over: on an error, or with
            // `Ok` once every command sender is gone. Calling it again after
            // `Ok` would return immediately, every time, pinning a core.
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = poller.run() => {
                    trace!("Polling has ended");
                    v
                }
            }
        });

        Ok(BcConnection {
            sink: sinker,
            poll_commander,
            rx_thread: RwLock::new(rx_thread),
            cancel,
            closing: AtomicBool::new(false),
            next_route_id: AtomicU64::new(0),
        })
    }

    pub(super) async fn send(&self, bc: Bc) -> crate::Result<()> {
        if self.closing.load(Ordering::Acquire) {
            return Err(Error::ConnectionShutdown);
        }
        self.sink.send(Outgoing::Bc(Box::new(bc))).await?;
        Ok(())
    }

    /// A route for a new subscriber, and the queue it reads
    fn new_route(&self, capacity: usize, overflow: Overflow) -> (Route, Receiver<Result<Bc>>) {
        // One slot more than asked for, kept for the error that ends it
        let (tx, rx) = channel(capacity + 1);
        let id = self.next_route_id.fetch_add(1, Ordering::Relaxed);
        (Route { tx, overflow, id }, rx)
    }

    /// Subscribes to the replies to a request. The subscription ends with
    /// [`Error::SubscriptionOverflow`] if 100 of them go unread.
    pub async fn subscribe(&self, msg_id: u32, msg_num: u16) -> Result<BcSubscription<'_>> {
        self.subscribe_opts(msg_id, msg_num, REPLY_CAPACITY, Overflow::Close)
            .await
    }

    /// Subscribes to the messages with this ID and number, holding up to
    /// `capacity` unread ones before `overflow` applies
    pub async fn subscribe_opts(
        &self,
        msg_id: u32,
        msg_num: u16,
        capacity: usize,
        overflow: Overflow,
    ) -> Result<BcSubscription<'_>> {
        let (route, rx) = self.new_route(capacity, overflow);
        self.poll_commander
            .send(PollCommand::AddSubscriber(msg_id, Some(msg_num), route))
            .await?;
        Ok(BcSubscription::new(rx, Some(msg_num as u32), self))
    }

    /// Some messages are initiated by the camera. This creates a handler for them
    /// It requires a closure that will be used to handle the message
    /// and return either None or Some(Bc) reply
    ///
    /// The handler stays until the returned guard is dropped. A message can
    /// only have one handler at a time: registering a second one is an error.
    pub async fn handle_msg<T>(&self, msg_id: u32, handler: T) -> Result<MsgHandlerGuard>
    where
        T: 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>,
    {
        let handler: Arc<MsgHandler> = Arc::new(handler);
        let (route, mut rx) = self.new_route(HANDLER_CAPACITY, Overflow::DropNewest);
        let route_id = route.id;
        self.add_id_route(msg_id, route).await?;

        // One worker per message ID handles them in the order they came.
        // A task per message used to pile up, unbounded, behind a slow
        // handler.
        let sink = self.sink.clone();
        let worker = tokio::task::spawn(async move {
            while let Some(Ok(bc)) = rx.recv().await {
                if let Some(reply) = handler(&bc).await {
                    debug_assert_eq!(reply.meta.msg_num, bc.meta.msg_num);
                    if sink.send(Outgoing::Bc(Box::new(reply))).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(MsgHandlerGuard {
            msg_id,
            route_id,
            poll_commander: self.poll_commander.clone(),
            worker: Some(worker),
        })
    }

    async fn add_id_route(&self, msg_id: u32, route: Route) -> Result<()> {
        let (reply, added) = oneshot::channel();
        self.poll_commander
            .send(PollCommand::AddIdRoute {
                msg_id,
                route,
                reply,
            })
            .await?;
        added.await.map_err(|_| Error::DroppedConnection)?
    }

    /// Some times we want to wait for a reply on a new message ID
    /// to do this we wait for the next packet with a certain ID
    /// grab it's message ID and then subscribe to that ID
    ///
    /// The command Snap that grabs a jpeg payload is an example of this
    ///
    /// This function creates a temporary handle to grab this single message
    pub async fn subscribe_to_id(
        &self,
        msg_id: u32,
        capacity: usize,
    ) -> Result<BcSubscription<'_>> {
        let (route, rx) = self.new_route(capacity, Overflow::Close);
        self.poll_commander
            .send(PollCommand::AddSubscriber(msg_id, None, route))
            .await?;
        Ok(BcSubscription::new(rx, None, self))
    }

    /// Subscribes to every message with this ID, whatever its number, such as
    /// the motion alarms the camera sends. A subscription by ID with
    /// [`Self::subscribe_to_id`] only sees the number of its first message.
    pub async fn subscribe_events(
        &self,
        msg_id: u32,
        capacity: usize,
    ) -> Result<BcSubscription<'_>> {
        let (route, rx) = self.new_route(capacity, Overflow::Close);
        self.add_id_route(msg_id, route).await?;
        Ok(BcSubscription::new_events(rx, self))
    }

    pub(crate) async fn join(&self) -> Result<()> {
        let mut locked_threads = self.rx_thread.write().await;
        while let Some(res) = locked_threads.join_next().await {
            match res {
                Err(e) => {
                    locked_threads.abort_all();
                    return Err(e.into());
                }
                Ok(Err(e)) => {
                    locked_threads.abort_all();
                    return Err(e);
                }
                Ok(Ok(())) => {}
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        // Refuse anything new, but let what is already queued, such as a
        // logout, go out first. Cancelling at once used to drop it.
        self.closing.store(true, Ordering::Release);
        let (flushed, written) = oneshot::channel();
        let _ = timeout(FLUSH_TIMEOUT, async {
            if self.sink.send(Outgoing::Flush(flushed)).await.is_ok() {
                let _ = written.await;
            }
        })
        .await;
        // Never wait on a full command queue
        let _ = self.poll_commander.try_send(PollCommand::Disconnect);
        self.cancel.cancel();
        let mut locked_threads = self.rx_thread.write().await;
        while locked_threads.join_next().await.is_some() {}
        Ok(())
    }
}

impl Drop for BcConnection {
    fn drop(&mut self) {
        log::trace!("Drop BcConnection");
        self.cancel.cancel();

        let poll_commander = self.poll_commander.clone();
        let mut threads = std::mem::take(&mut self.rx_thread);
        crate::spawn_cleanup(async move {
            let _ = poll_commander.send(PollCommand::Disconnect).await;
            let locked_threads = threads.get_mut();
            while locked_threads.join_next().await.is_some() {}
            log::trace!("Dropped BcConnection");
        });
    }
}

/// Keeps a handler made with [`BcConnection::handle_msg`]. Dropping it removes
/// the handler, so that the message can be handled anew.
pub struct MsgHandlerGuard {
    msg_id: u32,
    route_id: u64,
    poll_commander: Sender<PollCommand>,
    worker: Option<JoinHandle<()>>,
}

impl MsgHandlerGuard {
    /// Keeps the handler for as long as the connection lasts
    pub fn forever(mut self) {
        // Detached: dropping the handle leaves the worker running
        self.worker.take();
    }
}

impl Drop for MsgHandlerGuard {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.abort();
            // Queued, so it comes before any registration made after this
            let _ = self.poll_commander.try_send(PollCommand::RemoveIdRoute {
                msg_id: self.msg_id,
                route_id: self.route_id,
            });
        }
    }
}

enum PollCommand {
    Bc(Box<Result<Bc>>),
    AddSubscriber(u32, Option<u16>, Route),
    AddIdRoute {
        msg_id: u32,
        route: Route,
        reply: oneshot::Sender<Result<()>>,
    },
    RemoveIdRoute {
        msg_id: u32,
        route_id: u64,
    },
    Disconnect,
}

impl std::fmt::Debug for PollCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollCommand::Bc(_) => f.write_str("PollCommand::Bc"),
            PollCommand::AddSubscriber(_, _, _) => f.write_str("PollCommand::AddSubscriber"),
            PollCommand::AddIdRoute { .. } => f.write_str("PollCommand::AddIdRoute"),
            PollCommand::RemoveIdRoute { .. } => f.write_str("PollCommand::RemoveIdRoute"),
            PollCommand::Disconnect => f.write_str("PollCommand::Disconnect"),
        }
    }
}

struct Poller {
    routes: Routes,
    reciever: ReceiverStream<PollCommand>,
}

impl Poller {
    async fn run(&mut self) -> Result<()> {
        while let Some(command) = self.reciever.next().await {
            self.routes.prune();
            match command {
                PollCommand::Bc(boxed_response) => match *boxed_response {
                    Ok(response) => self.routes.deliver(response),
                    Err(e) => {
                        self.routes.fail_all(&e);
                        return Err(e);
                    }
                },
                PollCommand::AddSubscriber(msg_id, msg_num, route) => {
                    match self.routes.num.entry(msg_id).or_default().entry(msg_num) {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(route);
                        }
                        Entry::Occupied(mut occ_entry) if occ_entry.get().tx.is_closed() => {
                            occ_entry.insert(route);
                        }
                        Entry::Occupied(_) => {
                            route.fail(Error::SimultaneousSubscription { msg_num });
                        }
                    };
                }
                PollCommand::AddIdRoute {
                    msg_id,
                    route,
                    reply,
                } => {
                    let added = match self.routes.id.entry(msg_id) {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(route);
                            Ok(())
                        }
                        Entry::Occupied(mut occ_entry) if occ_entry.get().tx.is_closed() => {
                            occ_entry.insert(route);
                            Ok(())
                        }
                        // The caller hears of it. This used to end polling,
                        // and with it the connection.
                        Entry::Occupied(_) => Err(Error::SimultaneousSubscriptionId { msg_id }),
                    };
                    let _ = reply.send(added);
                }
                PollCommand::RemoveIdRoute { msg_id, route_id } => {
                    if self
                        .routes
                        .id
                        .get(&msg_id)
                        .is_some_and(|route| route.id == route_id)
                    {
                        self.routes.id.remove(&msg_id);
                    }
                }
                PollCommand::Disconnect => {
                    self.routes.fail_all(&Error::ConnectionShutdown);
                    return Err(Error::ConnectionShutdown);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::FutureExt;
    use std::time::Duration;
    use tokio_util::sync::PollSender;

    /// `Poller::run` returns `Ok` as soon as its command channel is closed.
    /// The task driving it must therefore not call it again in a loop, or it
    /// spins without ever yielding.
    #[tokio::test]
    async fn poller_run_returns_once_commands_close() {
        let (commander, commanded) = channel::<PollCommand>(1);
        let mut poller = Poller {
            routes: Default::default(),
            reciever: ReceiverStream::new(commanded),
        };
        drop(commander);
        let result = tokio::time::timeout(Duration::from_secs(1), poller.run())
            .await
            .expect("Poller::run did not return after its commands closed");
        assert!(result.is_ok());
    }

    /// A connection to a pretend camera: what the camera sends goes in
    /// through the first channel, what neolink sends comes out of the second
    async fn connection(writes: usize) -> (BcConnection, Sender<Result<Bc>>, Receiver<Bc>) {
        let (camera_tx, camera_rx) = channel(100);
        let (written_tx, written_rx) = channel(writes);
        let sink = PollSender::new(written_tx).sink_map_err(|_| Error::DroppedConnection);
        let connection =
            BcConnection::new(Box::new(sink), Box::new(ReceiverStream::new(camera_rx)))
                .await
                .unwrap();
        (connection, camera_tx, written_rx)
    }

    fn msg(msg_id: u32, msg_num: u16) -> Bc {
        Bc {
            meta: BcMeta {
                msg_id,
                channel_id: 0,
                msg_num,
                stream_type: 0,
                response_code: 200,
                class: 0x0000,
            },
            body: BcBody::ModernMsg(ModernMsg::default()),
        }
    }

    async fn soon<F: std::future::Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(2), future)
            .await
            .expect("this should not wait")
    }

    #[tokio::test]
    async fn a_subscriber_that_falls_behind_holds_up_no_one_else() {
        let (connection, camera, _written) = connection(10).await;
        let mut video = connection
            .subscribe_opts(MSG_ID_VIDEO, 1, 2, Overflow::Close)
            .await
            .unwrap();
        let mut ping = connection.subscribe(MSG_ID_PING, 2).await.unwrap();

        for _ in 0..10 {
            camera.send(Ok(msg(MSG_ID_VIDEO, 1))).await.unwrap();
        }
        camera.send(Ok(msg(MSG_ID_PING, 2))).await.unwrap();

        // The ping gets through although the video was never read
        assert!(soon(ping.recv()).await.is_ok());
        // And the video subscription ends after what it could hold
        assert!(soon(video.recv()).await.is_ok());
        assert!(soon(video.recv()).await.is_ok());
        assert!(matches!(
            soon(video.recv()).await,
            Err(Error::SubscriptionOverflow {
                msg_id: MSG_ID_VIDEO
            })
        ));

        // Only that one route closed
        camera.send(Ok(msg(MSG_ID_PING, 2))).await.unwrap();
        assert!(soon(ping.recv()).await.is_ok());
    }

    #[tokio::test]
    async fn a_closed_connection_reaches_every_waiter() {
        let (connection, camera, _written) = connection(10).await;
        let mut first = connection.subscribe(MSG_ID_PING, 1).await.unwrap();
        let mut second = connection.subscribe(MSG_ID_VERSION, 2).await.unwrap();

        // The camera closes the connection (TCP FIN)
        drop(camera);

        assert!(matches!(
            soon(first.recv()).await,
            Err(Error::DroppedConnection)
        ));
        assert!(matches!(
            soon(second.recv()).await,
            Err(Error::DroppedConnection)
        ));
        assert!(soon(connection.join()).await.is_err());
    }

    #[tokio::test]
    async fn shutdown_sends_what_is_queued_first() {
        let (connection, _camera, mut written) = connection(10).await;
        connection.send(msg(MSG_ID_LOGOUT, 1)).await.unwrap();

        soon(connection.shutdown()).await.unwrap();

        assert_eq!(written.recv().await.unwrap().meta.msg_id, MSG_ID_LOGOUT);
        assert!(matches!(
            connection.send(msg(MSG_ID_PING, 2)).await,
            Err(Error::ConnectionShutdown)
        ));
    }

    #[tokio::test]
    async fn shutdown_ends_even_if_the_camera_takes_nothing() {
        // Nothing reads what neolink writes, so its queue stays full
        let (connection, _camera, _written) = connection(1).await;
        for num in 0..50 {
            connection.send(msg(MSG_ID_PING, num)).await.unwrap();
        }

        tokio::time::timeout(
            FLUSH_TIMEOUT + Duration::from_secs(1),
            connection.shutdown(),
        )
        .await
        .expect("shutdown waited on the camera for good")
        .unwrap();
    }

    fn ignore(_: &Bc) -> BoxFuture<'_, Option<Bc>> {
        async { None }.boxed()
    }

    #[tokio::test]
    async fn a_handler_is_unique_until_its_guard_is_dropped() {
        let (connection, camera, _written) = connection(10).await;
        let guard = connection.handle_msg(MSG_ID_MOTION, ignore).await.unwrap();

        // A second one is refused, without ending the connection
        assert!(matches!(
            connection.handle_msg(MSG_ID_MOTION, ignore).await,
            Err(Error::SimultaneousSubscriptionId { .. })
        ));
        let mut ping = connection.subscribe(MSG_ID_PING, 1).await.unwrap();
        camera.send(Ok(msg(MSG_ID_PING, 1))).await.unwrap();
        assert!(soon(ping.recv()).await.is_ok());

        // And accepted once the first is gone
        drop(guard);
        assert!(connection.handle_msg(MSG_ID_MOTION, ignore).await.is_ok());
    }

    #[tokio::test]
    async fn a_handler_answers_the_camera() {
        let (connection, camera, mut written) = connection(10).await;
        connection
            .handle_msg(MSG_ID_UDP_KEEP_ALIVE, |bc| {
                let reply = msg(MSG_ID_UDP_KEEP_ALIVE, bc.meta.msg_num);
                async move { Some(reply) }.boxed()
            })
            .await
            .unwrap()
            .forever();

        for num in 0..3 {
            camera
                .send(Ok(msg(MSG_ID_UDP_KEEP_ALIVE, num)))
                .await
                .unwrap();
        }
        for num in 0..3 {
            assert_eq!(soon(written.recv()).await.unwrap().meta.msg_num, num);
        }
    }

    #[tokio::test]
    async fn an_event_subscription_sees_every_message_number() {
        let (connection, camera, _written) = connection(10).await;
        let mut events = connection
            .subscribe_events(MSG_ID_MOTION, 10)
            .await
            .unwrap();

        camera.send(Ok(msg(MSG_ID_MOTION, 7))).await.unwrap();
        camera.send(Ok(msg(MSG_ID_MOTION, 8))).await.unwrap();

        assert_eq!(soon(events.recv()).await.unwrap().meta.msg_num, 7);
        assert_eq!(soon(events.recv()).await.unwrap().meta.msg_num, 8);
    }
}
