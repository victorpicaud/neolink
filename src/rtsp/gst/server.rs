//! Attempts to subclass RtspServer
//!
//! We are now messing with gstreamer glib objects
//! expect issues

use super::AnyResult;
use crate::config::*;

use anyhow::{anyhow, Context};
use gstreamer::glib::{self, object_subclass, MainLoop, Object};
use gstreamer_rtsp::RTSPAuthMethod;
use gstreamer_rtsp_server::{
    gio::{Cancellable, TlsAuthenticationMode, TlsCertificate},
    prelude::*,
    subclass::prelude::*,
    RTSPAuth, RTSPFilterResult, RTSPServer, RTSPToken, RTSP_TOKEN_MEDIA_FACTORY_ROLE,
};
use log::*;
use std::{
    collections::{HashMap, HashSet},
    fs,
    sync::OnceLock,
};
use tokio::{
    sync::RwLock,
    time::{interval, Duration},
};
use tokio_util::sync::CancellationToken;

/// GStreamer serves every RTSP client from one shared thread by default, so a
/// client waiting on a slow camera held up all the others. Clients now get a
/// thread each, up to this many; beyond that they share threads again.
const MAX_CLIENT_THREADS: i32 = 64;

glib::wrapper! {
    /// The wrapped RTSPServer
    pub(crate) struct NeoRtspServer(ObjectSubclass<NeoRtspServerImpl>) @extends RTSPServer;
}

impl Default for NeoRtspServer {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

impl NeoRtspServer {
    pub(crate) fn new() -> AnyResult<Self> {
        gstreamer::init().context("Gstreamer failed to initialise")?;
        let factory = Object::new::<NeoRtspServer>();

        // Setup auth
        let auth = factory.auth().unwrap_or_default();
        auth.set_supported_methods(RTSPAuthMethod::Basic);
        let mut un_authtoken = RTSPToken::builder()
            .field(
                //RTSP_TOKEN_MEDIA_FACTORY_ROLE: Means look inside the media factory settings and use the same permissions this user (`"anonymous"`) has
                RTSP_TOKEN_MEDIA_FACTORY_ROLE,
                "anonymous",
            )
            .build();
        auth.set_default_token(Some(&mut un_authtoken));
        factory.set_auth(Some(&auth));

        if let Some(pool) = factory.thread_pool() {
            pool.set_max_threads(MAX_CLIENT_THREADS);
            log::debug!("RTSP clients get up to {} threads", pool.max_threads());
        }

        factory.connect_client_connected(|_, client| {
            client.connect_new_session(|_, session| {
                log::debug!("New Session");
                // Session timeout too small causes us to drop
                // some ffmpeg clients too soon
                // Too long causes too many open connections with
                // clients like frigate (that seem to open multiple
                //   connections without shutting down old ones)
                session.set_timeout(30);
            });
        });

        Ok(factory)
    }

    /// Starts serving until [`Self::quit`]. The returned guard quits when it
    /// is dropped, so the server stops however the code that started it ends.
    pub(crate) async fn run(&self, bind_addr: &str, bind_port: u16) -> AnyResult<QuitOnDrop> {
        let server = self;
        server.set_address(bind_addr);
        server.set_service(&format!("{}", bind_port));
        // Attach server to default Glib context. Binding happens here, so an
        // address or port that cannot be used fails startup instead of leaving
        // a server that never answers.
        server
            .create_source(None::<&Cancellable>)
            .with_context(|| format!("Could not listen for RTSP on {bind_addr}:{bind_port}"))?
            .attach(None);

        let main_loop = MainLoop::new(None, false);
        let stopped = CancellationToken::new();
        self.imp()
            .main_loop
            .set((main_loop.clone(), stopped.clone()))
            .map_err(|_| anyhow!("The RTSP server is already running"))?;

        // Run the Glib main loop on a thread of its own. On tokio's blocking
        // pool, a loop that nobody quit kept the runtime, and so neolink, from
        // ever exiting.
        let thread_stopped = stopped.clone();
        std::thread::Builder::new()
            .name("rtsp-main-loop".to_string())
            .spawn(move || {
                let _stopped = thread_stopped.drop_guard();
                main_loop.run();
            })
            .context("Could not start the RTSP main loop")?;

        let sessions_server = server.clone();
        tokio::task::spawn(async move {
            let mut ticks = interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = stopped.cancelled() => break,
                    _ = ticks.tick() => sessions_server.clean_up_sessions(),
                }
            }
        });
        Ok(QuitOnDrop(server.clone()))
    }

    /// Stops the main loop. Any thread can call it, any number of times.
    pub(crate) fn quit(&self) {
        if let Some((main_loop, _)) = self.imp().main_loop.get() {
            // Quit from inside the loop: a quit that came before the loop had
            // started running would be lost
            let main_loop = main_loop.clone();
            glib::idle_add_once(move || main_loop.quit());
        }
    }

    /// Waits for the main loop to stop
    pub(crate) async fn join(&self) -> AnyResult<()> {
        if let Some((_, stopped)) = self.imp().main_loop.get() {
            stopped.cancelled().await;
        }
        Ok(())
    }

    fn clean_up_sessions(&self) {
        if let Some(sessions) = self.session_pool() {
            let cleanups = sessions.cleanup();
            if cleanups > 0 {
                log::debug!("Cleaned up {cleanups} sessions");
            }
            sessions.filter(Some(&mut |_, session| {
                let remaining = session.next_timeout_usec(glib::monotonic_time());
                log::debug!(
                    "{:?}: {}/{}",
                    session.sessionid(),
                    remaining,
                    session.timeout(),
                );
                RTSPFilterResult::Keep
            }));
        }
    }

    pub(crate) fn set_up_tls(&self, config: &Config) -> AnyResult<()> {
        self.imp().set_up_tls(config)
    }

    pub(crate) async fn add_user(&self, username: &str, password: &str) -> AnyResult<()> {
        self.imp().add_user(username, password).await
    }

    pub(crate) async fn remove_user(&self, username: &str) -> AnyResult<()> {
        self.imp().remove_user(username).await
    }

    pub(crate) async fn get_users(&self) -> AnyResult<HashSet<String>> {
        self.imp().get_users().await
    }
}

unsafe impl Send for NeoRtspServer {}
unsafe impl Sync for NeoRtspServer {}

/// Quits the server's main loop when dropped
#[derive(Debug)]
#[must_use = "the server stops when this is dropped"]
pub(crate) struct QuitOnDrop(NeoRtspServer);

impl Drop for QuitOnDrop {
    fn drop(&mut self) {
        self.0.quit();
    }
}

#[derive(Default)]
pub(crate) struct NeoRtspServerImpl {
    users: RwLock<HashMap<String, String>>,
    /// The main loop once it runs, and a token cancelled when it stops
    main_loop: OnceLock<(MainLoop, CancellationToken)>,
}

impl ObjectImpl for NeoRtspServerImpl {}
impl RTSPServerImpl for NeoRtspServerImpl {}

#[object_subclass]
impl ObjectSubclass for NeoRtspServerImpl {
    const NAME: &'static str = "NeoRtspServer";
    type Type = NeoRtspServer;
    type ParentType = RTSPServer;
}

impl NeoRtspServerImpl {
    pub(crate) fn set_tls(
        &self,
        cert_file: &str,
        client_auth: TlsAuthenticationMode,
    ) -> AnyResult<()> {
        debug!("Setting up TLS using {}", cert_file);
        let auth = self.obj().auth().unwrap_or_default();

        // We seperate reading the file and changing to a PEM so that we get different error messages.
        let cert_contents = fs::read_to_string(cert_file).with_context(|| "TLS file not found")?;
        let cert = TlsCertificate::from_pem(&cert_contents)
            .with_context(|| "Not a valid TLS certificate")?;
        auth.set_tls_certificate(Some(&cert));
        auth.set_tls_authentication_mode(client_auth);

        self.obj().set_auth(Some(&auth));
        Ok(())
    }

    pub(crate) fn set_up_tls(&self, config: &Config) -> AnyResult<()> {
        let tls_client_auth = match &config.tls_client_auth as &str {
            "request" => TlsAuthenticationMode::Requested,
            "require" => TlsAuthenticationMode::Required,
            "none" => TlsAuthenticationMode::None,
            _ => unreachable!(),
        };
        if let Some(cert_path) = &config.certificate {
            self.set_tls(cert_path, tls_client_auth)
                .with_context(|| "Failed to set up TLS")?;
        }
        Ok(())
    }

    pub(crate) async fn add_user(&self, username: &str, password: &str) -> AnyResult<()> {
        let mut locked_users = self.users.write().await;
        let auth = self.obj().auth().unwrap();

        let token = RTSPToken::builder()
            .field(RTSP_TOKEN_MEDIA_FACTORY_ROLE, username)
            .build();
        let basic = RTSPAuth::make_basic(username, password);

        if let Some(old_basic) = locked_users.get(username) {
            if basic.as_str() == old_basic {
                // Password is the same
                return Ok(());
            } else {
                // Different password
                auth.remove_basic(old_basic);
            }
        }

        auth.add_basic(basic.as_str(), &token);

        locked_users.insert(username.to_string(), basic.to_string());
        Ok(())
    }

    pub(crate) async fn remove_user(&self, username: &str) -> AnyResult<()> {
        let mut locked_users = self.users.write().await;
        let auth = self.obj().auth().unwrap();

        if let Some(old_basic) = locked_users.get(username) {
            auth.remove_basic(old_basic);
        }

        locked_users.remove(username);
        Ok(())
    }

    pub(crate) async fn get_users(&self) -> AnyResult<HashSet<String>> {
        let locked_users = self.users.read().await;
        Ok(locked_users.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_port_in_use_fails_startup() -> AnyResult<()> {
        let taken = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = taken.local_addr()?.port();

        let error = NeoRtspServer::new()?
            .run("127.0.0.1", port)
            .await
            .expect_err("the port is already taken");
        assert!(
            format!("{error:#}").contains("Could not listen for RTSP"),
            "{error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_main_loop_stops_when_its_guard_is_dropped() -> AnyResult<()> {
        let server = NeoRtspServer::new()?;
        // Quit before the loop had a chance to start
        drop(server.run("127.0.0.1", 0).await?);
        tokio::time::timeout(Duration::from_secs(5), server.join())
            .await
            .context("the main loop is still running")??;
        Ok(())
    }
}
