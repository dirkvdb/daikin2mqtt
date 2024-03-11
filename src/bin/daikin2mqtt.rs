#![warn(clippy::unwrap_used)]
use core::time;
use std::path::PathBuf;

use clap::Parser;
use daikin2mqtt::{
    bridge::{self, DaikinMqttBridgeConfig},
    mqtt::MqttConfig,
};
use env_logger::Env;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const PACKAGE: &str = env!("CARGO_PKG_NAME");

#[derive(Parser, Debug)]
#[clap(name = "daikin2mqtt", about = "Interface between daikin heatpump and MQTT")]
struct Opt {
    #[clap(long = "daikin-poll-interval", env = "DAIKIN_POLL_INTERVAL", default_value_t = 600)]
    cloud_poll_interval: u64,

    // set the mqtt addr
    #[clap(long = "mqtt-addr", env = "DAIKIN_MQTT_ADDRESS")]
    mqtt_addr: String,

    #[clap(long = "mqtt-user", env = "DAIKIN_MQTT_USER")]
    mqtt_user: Option<String>,

    #[clap(long = "mqtt-pass", env = "DAIKIN_MQTT_PASS")]
    mqtt_password: Option<String>,

    #[clap(long = "mqtt-port", env = "DAIKIN_MQTT_PORT", default_value_t = 1883)]
    mqtt_port: u16,

    #[clap(long = "mqtt-client-id", env = "DAIKIN_CLIENT_ID", default_value_t = String::from("daikin2mqtt"))]
    mqtt_client_id: String,

    #[clap(long = "mqtt-base-topic", env = "DAIKIN_MQTT_BASE_TOPIC", default_value_t = String::from("daikin"))]
    mqtt_base_topic: String,

    // #[clap(
    //     long = "hass-discovery",
    //     env = "DAIKIN_HASS_DISCOVERY",
    //     default_value_t = false
    // )]
    // hass_discovery: bool,
    #[clap(long = "data-dir", env = "DAIKIN_DATA_DIR", default_value_t = String::from("./data"))]
    data_dir: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let opt = Opt::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();

    log::info!("{} version {}", PACKAGE, VERSION);

    let cfg = DaikinMqttBridgeConfig {
        mqtt_config: MqttConfig {
            server: opt.mqtt_addr,
            port: opt.mqtt_port,
            client_id: opt.mqtt_client_id,
            user: opt.mqtt_user.unwrap_or(String::new()),
            password: opt.mqtt_password.unwrap_or(String::new()),
            base_topic: opt.mqtt_base_topic,
        },
        //hass_discovery: opt.hass_discovery,
        data_dir: PathBuf::from(opt.data_dir),
        poll_interval: time::Duration::from_secs(opt.cloud_poll_interval),
    };

    bridge::DaikinMqttBridge::new(cfg)
        .run()
        .await
        .expect("Failed to run bridge");
}
