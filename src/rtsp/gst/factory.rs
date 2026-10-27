//! Attempts to subclass GstMediaFactory
//!
//! We are now messing with gstreamer glib objects
//! expect issues

use super::AnyResult;
use anyhow::anyhow;
use gstreamer::glib::object_subclass;
use gstreamer::Element;
use gstreamer::{
    glib::{self, Object},
    Structure,
};
use gstreamer_rtsp::RTSPUrl;
use gstreamer_rtsp_server::prelude::*;
use gstreamer_rtsp_server::subclass::prelude::*;
use gstreamer_rtsp_server::RTSPTransportMode;
use gstreamer_rtsp_server::{RTSPMedia, RTSPMediaFactory};
use gstreamer_rtsp_server::{RTSP_PERM_MEDIA_FACTORY_ACCESS, RTSP_PERM_MEDIA_FACTORY_CONSTRUCT};
use log::*;
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::OnceLock;

/// Builds the pipeline for one client: a bin holding its `payN` payloaders,
/// or an error to refuse the client
type BuildPipeline = dyn Fn() -> AnyResult<Element> + Send + Sync;

glib::wrapper! {
    /// The wrapped RTSPMediaFactory
    pub(crate) struct NeoMediaFactory(ObjectSubclass<NeoMediaFactoryImpl>) @extends RTSPMediaFactory;
}

impl Default for NeoMediaFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl NeoMediaFactory {
    fn new() -> Self {
        let factory = Object::new::<NeoMediaFactory>();
        factory.set_shared(false);
        factory.set_eos_shutdown(false);
        factory.set_stop_on_disconnect(false);
        // factory.set_publish_clock_mode(gstreamer_rtsp_server::RTSPPublishClockMode::Clock);
        factory.set_suspend_mode(gstreamer_rtsp_server::RTSPSuspendMode::Reset);
        factory.set_transport_mode(RTSPTransportMode::PLAY);
        factory
    }

    pub(crate) fn new_with_callback<F>(callback: F) -> Self
    where
        F: Fn() -> AnyResult<Element> + Send + Sync + 'static,
    {
        let factory = Self::new();
        factory.imp().set_callback(callback);
        factory
    }

    pub(crate) fn add_permitted_roles<T: AsRef<str>>(&self, permitted_roles: &HashSet<T>) {
        for permitted_role in permitted_roles {
            let s = permitted_role.as_ref();
            log::debug!("Adding {} as permitted user", s);
            self.add_role_from_structure(
                &Structure::builder(s)
                    .field(RTSP_PERM_MEDIA_FACTORY_ACCESS, true)
                    .field(RTSP_PERM_MEDIA_FACTORY_CONSTRUCT, true)
                    .build(),
            );
        }
        // During auth, first it binds anonymously. At this point it checks
        // RTSP_PERM_MEDIA_FACTORY_ACCESS to see if anyone can connect
        // This is done before the auth token is loaded, possibliy an upstream bug there
        // After checking RTSP_PERM_MEDIA_FACTORY_ACCESS anonymously
        // It loads the auth token of the user and checks that users
        // RTSP_PERM_MEDIA_FACTORY_CONSTRUCT allowing them to play
        // As a result of this we must ensure that if anonymous is not granted RTSP_PERM_MEDIA_FACTORY_ACCESS
        // As a part of permitted users then we must allow it to access
        // at least RTSP_PERM_MEDIA_FACTORY_ACCESS but not RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        // Watching Actually happens during RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        // So this should be OK to do.
        // FYI: If no RTSP_PERM_MEDIA_FACTORY_ACCESS then server returns 404 not found
        //      If yes RTSP_PERM_MEDIA_FACTORY_ACCESS but no RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        //        server returns 401 not authourised
        if !permitted_roles
            .iter()
            .map(|i| i.as_ref())
            .collect::<HashSet<&str>>()
            .contains(&"anonymous")
        {
            self.add_role_from_structure(
                &Structure::builder("anonymous")
                    .field(RTSP_PERM_MEDIA_FACTORY_ACCESS, true)
                    .build(),
            );
        }
    }
}

unsafe impl Send for NeoMediaFactory {}
unsafe impl Sync for NeoMediaFactory {}

#[derive(Default)]
pub(crate) struct NeoMediaFactoryImpl {
    /// Set once, so that calling it takes no lock: it can wait on the camera
    /// for a while
    call_back: OnceLock<Box<BuildPipeline>>,
}

thread_local! {
    /// The pipeline `construct` built, until GStreamer asks for it through
    /// `create_element` on the same thread
    static BUILT: RefCell<Option<Element>> = const { RefCell::new(None) };
}

impl NeoMediaFactoryImpl {
    fn set_callback<F>(&self, callback: F)
    where
        F: Fn() -> AnyResult<Element> + Send + Sync + 'static,
    {
        // Only `new_with_callback` sets it, right after creating the factory
        let _ = self.call_back.set(Box::new(callback));
    }

    fn build_pipeline(&self) -> AnyResult<Element> {
        let build = self
            .call_back
            .get()
            .ok_or_else(|| anyhow!("The media factory has no pipeline builder"))?;
        build()
    }
}

impl ObjectImpl for NeoMediaFactoryImpl {}
impl RTSPMediaFactoryImpl for NeoMediaFactoryImpl {
    fn construct(&self, url: &RTSPUrl) -> Option<RTSPMedia> {
        // Build the pipeline before GStreamer asks for the element. A client
        // that cannot be served then gets no media, which GStreamer answers
        // with an error reply, rather than a NULL element, which it also logs
        // as GLib CRITICALs.
        let element = match self.build_pipeline() {
            Ok(element) => element,
            Err(e) => {
                warn!("Refusing an RTSP client of {}: {e:#}", url.request_uri());
                return None;
            }
        };
        BUILT.with(|built| built.replace(Some(element)));
        let media = self.parent_construct(url);
        BUILT.with(|built| built.take());
        media
    }

    fn create_element(&self, _url: &RTSPUrl) -> Option<Element> {
        // Called from the parent's `construct`, which `construct` chains up to
        BUILT.with(|built| built.take())
    }
}

#[object_subclass]
impl ObjectSubclass for NeoMediaFactoryImpl {
    const NAME: &'static str = "NeoMediaFactory";
    type Type = super::NeoMediaFactory;
    type ParentType = RTSPMediaFactory;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn url() -> RTSPUrl {
        gstreamer::init().unwrap();
        RTSPUrl::parse("rtsp://127.0.0.1:8554/cam/main")
            .1
            .expect("a valid URL")
    }

    #[test]
    fn a_refused_client_gets_no_media_and_no_criticals() {
        let url = url();
        let criticals = Arc::new(AtomicUsize::new(0));
        let counter = criticals.clone();
        let handler = glib::log_set_handler(
            Some("GLib-GObject"),
            glib::LogLevels::LEVEL_CRITICAL,
            false,
            false,
            move |_, _, _| {
                counter.fetch_add(1, Ordering::SeqCst);
            },
        );

        let factory = NeoMediaFactory::new_with_callback(|| Err(anyhow!("not ready")));
        let media = factory.construct(&url);

        glib::log_remove_handler(Some("GLib-GObject"), handler);
        assert!(media.is_err(), "a refused client must get no media");
        assert_eq!(criticals.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_served_client_gets_the_pipeline_that_was_built() {
        let url = url();
        let factory = NeoMediaFactory::new_with_callback(|| {
            let bin = gstreamer::Bin::new();
            let source = gstreamer::ElementFactory::make("videotestsrc").build()?;
            let payload = gstreamer::ElementFactory::make("rtpvrawpay")
                .name("pay0")
                .build()?;
            bin.add_many([&source, &payload])?;
            source.link(&payload)?;
            Ok(bin.upcast())
        });

        let media = factory.construct(&url).expect("a media for the client");
        // GStreamer hands it over locked, for the client to configure
        media.unlock();
        assert_eq!(media.n_streams(), 1);
    }
}
