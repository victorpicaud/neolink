//! Contains code that is not specific to any of the subcommands
//!
use log::*;

use super::config::CameraConfig;
use anyhow::{anyhow, Context, Error, Result};
use neolink_core::bc_protocol::{
    BcCamera, BcCameraOpt, ConnectionProtocol, Credentials, DiscoveryMethods, MaxEncryption,
};
use std::{
    fmt::{Display, Error as FmtError, Formatter},
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};

pub(crate) fn timeout<F>(future: F) -> tokio::time::Timeout<F>
where
    F: std::future::Future,
{
    tokio::time::timeout(tokio::time::Duration::from_secs(15), future)
}

/// How long a name may take to resolve
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
/// How long connecting to a camera and logging in may take, discovery
/// included. A connection attempt that hung otherwise held up the camera
/// until neolink restarted.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

/// Resolves a camera's address: a host and port, or a host alone. Without
/// blocking the runtime: a slow DNS server held up a tokio worker, and
/// everything that ran on it.
async fn resolve(address: &str) -> Result<(Option<u16>, Vec<IpAddr>)> {
    if let Ok(ip) = IpAddr::from_str(address) {
        return Ok((None, vec![ip]));
    }
    let lookup = async {
        match tokio::net::lookup_host(address).await {
            Ok(addrs) => {
                let addrs: Vec<SocketAddr> = addrs.collect();
                Ok((addrs.first().map(SocketAddr::port), addrs))
            }
            // A host without a port
            Err(_) => Ok((None, tokio::net::lookup_host((address, 0)).await?.collect())),
        }
    };
    let (port, addrs): (Option<u16>, Vec<SocketAddr>) =
        tokio::time::timeout(LOOKUP_TIMEOUT, lookup)
            .await
            .map_err(|_| anyhow!("Looking up {address} took too long"))?
            .map_err(|e: std::io::Error| anyhow!("Could not look up {address}: {e}"))?;
    Ok((port, addrs.iter().map(SocketAddr::ip).collect()))
}

/// Runs the async part of a `Drop` on the current tokio runtime. Without one,
/// as while the process exits, the cleanup is dropped instead: `Handle::current`
/// would panic, and a panic in a `Drop` can abort the process.
pub(crate) fn spawn_cleanup<F>(cleanup: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(cleanup);
    }
}

pub(crate) enum AddressOrUid {
    Address(String),
    #[allow(dead_code)]
    Uid(String, DiscoveryMethods),
    #[allow(dead_code)]
    AddressWithUid(String, String, DiscoveryMethods),
}

impl Display for AddressOrUid {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            AddressOrUid::AddressWithUid(addr, uid, _) => {
                write!(f, "Address: {}, UID: {}", addr, uid)
            }
            AddressOrUid::Address(host) => write!(f, "Address: {}", host),
            AddressOrUid::Uid(host, _) => write!(f, "UID: {}", host),
        }
    }
}

impl AddressOrUid {
    // Created by translating the config fields directly
    pub(crate) fn new(
        address: &Option<String>,
        uid: &Option<String>,
        method: &DiscoveryMethods,
    ) -> Result<Self, Error> {
        match (address, uid) {
            (None, None) => Err(anyhow!("Neither address or uid given")),
            (Some(host), Some(uid)) => Ok(AddressOrUid::AddressWithUid(
                host.clone(),
                uid.clone(),
                *method,
            )),
            (Some(host), None) => Ok(AddressOrUid::Address(host.clone())),
            (None, Some(host)) => Ok(AddressOrUid::Uid(host.clone(), *method)),
        }
    }

    // Convience method to get the BcCamera with the appropiate method
    // from a camera_config
    pub(crate) async fn connect_camera(
        &self,
        camera_config: &CameraConfig,
    ) -> Result<BcCamera, Error> {
        let (port, addrs) = match camera_config.camera_addr.as_ref() {
            Some(address) => resolve(address).await?,
            None => (None, vec![]),
        };

        let options = BcCameraOpt {
            name: camera_config.name.clone(),
            channel_id: camera_config.channel_id,
            addrs,
            port,
            uid: camera_config.camera_uid.clone(),
            protocol: ConnectionProtocol::TcpUdp,
            discovery: camera_config.discovery,
            credentials: Credentials {
                username: camera_config.username.clone(),
                password: camera_config.password.clone(),
            },
            debug: camera_config.debug,
            max_discovery_retries: camera_config.max_discovery_retries,
        };

        trace!("Camera Info: {:?}", options);

        Ok(BcCamera::new(&options).await?)
    }
}

pub(crate) async fn connect_and_login(camera_config: &CameraConfig) -> Result<BcCamera> {
    let camera_addr = AddressOrUid::new(
        &camera_config.camera_addr,
        &camera_config.camera_uid,
        &camera_config.discovery,
    )
    .unwrap();
    info!(
        "{}: Connecting to camera at {}",
        camera_config.name, camera_addr
    );

    let camera = tokio::time::timeout(CONNECT_TIMEOUT, camera_addr.connect_camera(camera_config))
        .await
        .map_err(|_| anyhow!("Connecting took over {}s", CONNECT_TIMEOUT.as_secs()))
        .and_then(|connected| connected)
        .with_context(|| {
            format!(
                "Failed to connect to camera {} at {} on channel {}",
                camera_config.name, camera_addr, camera_config.channel_id
            )
        })?;

    let max_encryption = match camera_config.max_encryption.to_lowercase().as_str() {
        "none" => MaxEncryption::None,
        "bcencrypt" => MaxEncryption::BcEncrypt,
        "aes" => MaxEncryption::Aes,
        _ => MaxEncryption::Aes,
    };
    info!("{}: Logging in", camera_config.name);
    timeout(camera.login_with_maxenc(max_encryption))
        .await
        .with_context(|| format!("Failed to login to {}", camera_config.name))??;

    info!("{}: Connected and logged in", camera_config.name);

    Ok(camera)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn addresses_resolve_with_or_without_a_port() -> Result<()> {
        let localhost = IpAddr::from_str("127.0.0.1")?;
        assert_eq!(resolve("127.0.0.1").await?, (None, vec![localhost]));

        let (port, addrs) = resolve("localhost:9000").await?;
        assert_eq!(port, Some(9000));
        assert!(addrs.iter().any(IpAddr::is_loopback), "{addrs:?}");

        let (port, addrs) = resolve("localhost").await?;
        assert_eq!(port, None);
        assert!(addrs.iter().any(IpAddr::is_loopback), "{addrs:?}");

        assert!(resolve("no-such-camera.invalid").await.is_err());
        Ok(())
    }
}
