use anyhow::{Context, Result};
use clap::Parser;
use fcm_push_listener::{new_heartbeat_ack, Message, MessageStream, Registration};
use log::*;
use std::{fs, path::PathBuf};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use validator::Validate;

mod config;
mod opt;
mod utils;

use config::Config;
use opt::Opt;
use utils::find_and_connect;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let opt = Opt::parse();

    let conf_path = opt.config.context("Must supply --config file")?;
    let config: Config = toml::from_str(
        &fs::read_to_string(&conf_path)
            .with_context(|| format!("Failed to read {:?}", conf_path))?,
    )
    .with_context(|| format!("Failed to parse the {:?} config file", conf_path))?;

    config
        .validate()
        .with_context(|| format!("Failed to validate the {:?} config file", conf_path))?;

    let camera = find_and_connect(&config, &opt.camera).await?;

    // 696841269229 is the reo_iphone FCM Sender_ID
    // let registration = fcm_push_listener::register("696841269229").await?;
    // 743639030586 is the reo_fcm FCM Sender_ID
    // 263684512460 is my test Sender_ID
    // let registration = fcm_push_listener::register("743639030586").await?
    // I have confirmed that I can recieve test messages with this SenderID
    // into this program
    // let registration = fcm_push_listener::register("263684512460").await?;

    // Reolink's Android app, as Firebase knows it. Registering by sender ID
    // alone is what Google stopped taking in June 2024.
    let firebase_app_id = "1:743639030586:android:86f60a4fb7143876";
    let firebase_project_id = "reolink-login";
    let firebase_api_key = "AIzaSyBEUIuWHnnOEwFahxWgQB4Yt4NsgOmkPyE";

    let http = reqwest::Client::new();
    let token_path = PathBuf::from("./token.toml");
    let registration = if let Ok(Ok(registration)) =
        fs::read_to_string(&token_path).map(|v| toml::from_str::<Registration>(&v))
    {
        info!("Loaded token");
        registration
    } else {
        info!("Registering new token");
        let registration = fcm_push_listener::register(
            &http,
            firebase_app_id,
            firebase_project_id,
            firebase_api_key,
            None,
        )
        .await?;
        let new_token = toml::to_string(&registration)?;
        fs::write(token_path, new_token)?;
        registration
    };

    // Send registration.fcm_token to the server to allow it to send push messages to you.
    info!("registration.fcm_token: {}", registration.fcm_token);
    let uid = "6A5443E486511B0D828543445DC55A7D"; // MD5 Hash of "WHY_REOLINK"
    camera
        .send_pushinfo_android(&registration.fcm_token, uid)
        .await?;

    info!("Listening");
    let session = registration.gcm.checkin(&http).await?;
    let connection = session.new_connection(vec![]).await?;
    let mut messages = MessageStream::wrap(connection, &registration.keys);
    while let Some(message) = messages.next().await {
        match message? {
            Message::Data(data) => {
                info!("Message JSON: {}", String::from_utf8_lossy(&data.body));
                info!("Persistent ID: {:?}", data.persistent_id);
            }
            Message::HeartbeatPing => messages.write_all(&new_heartbeat_ack()).await?,
            Message::Other(..) => {}
        }
    }
    Ok(())
}
