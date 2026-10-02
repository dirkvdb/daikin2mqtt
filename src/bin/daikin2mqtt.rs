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

#[derive(Parser)]
#[clap(name = "daikin2mqtt", about = "Interface between daikin heatpump and MQTT")]
struct Opt {
    #[clap(long = "daikin-client-id", env = "DAIKIN_OAUTH_CLIENT_ID")]
    cloud_client_id: String,

    #[clap(
        long = "daikin-client-secret",
        env = "DAIKIN_OAUTH_CLIENT_SECRET",
        hide_env_values = true
    )]
    cloud_client_secret: String,

    #[clap(
        long = "daikin-redirect-uri",
        env = "DAIKIN_REDIRECT_URI",
        default_value = "onectaintegration://callback/"
    )]
    cloud_redirect_uri: String,

    #[clap(long = "daikin-poll-interval", env = "DAIKIN_POLL_INTERVAL", default_value_t = 600)]
    cloud_poll_interval: u64,

    // set the mqtt addr
    #[clap(long = "mqtt-addr", env = "DAIKIN_MQTT_ADDRESS")]
    mqtt_addr: String,

    #[clap(long = "mqtt-user", env = "DAIKIN_MQTT_USER")]
    mqtt_user: Option<String>,

    #[clap(long = "mqtt-pass", env = "DAIKIN_MQTT_PASS", hide_env_values = true)]
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
        cloud_client_id: opt.cloud_client_id,
        cloud_client_secret: opt.cloud_client_secret,
        cloud_redirect_uri: opt.cloud_redirect_uri,
        //hass_discovery: opt.hass_discovery,
        data_dir: PathBuf::from(opt.data_dir),
        poll_interval: time::Duration::from_secs(opt.cloud_poll_interval),
    };

    bridge::DaikinMqttBridge::new(cfg)
        .run()
        .await
        .expect("Failed to run bridge");
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{error::ErrorKind, CommandFactory, FromArgMatches};

    fn parse_args(args: &[&str]) -> Result<Opt, clap::Error> {
        let matches = Opt::command()
            .mut_args(|arg| arg.env(None::<&str>))
            .try_get_matches_from(args)?;
        Opt::from_arg_matches(&matches)
    }

    #[test_log::test]
    fn parses_oauth_credentials_with_default_redirect_uri() {
        let opt = parse_args(&[
            "daikin2mqtt",
            "--mqtt-addr",
            "localhost",
            "--daikin-client-id",
            "test-client",
            "--daikin-client-secret",
            "test-secret",
        ])
        .expect("valid OAuth options");

        assert_eq!(opt.cloud_client_id, "test-client");
        assert_eq!(opt.cloud_client_secret, "test-secret");
        assert_eq!(opt.cloud_redirect_uri, "onectaintegration://callback/");
    }

    #[test_log::test]
    fn parses_custom_redirect_uri() {
        let opt = parse_args(&[
            "daikin2mqtt",
            "--mqtt-addr",
            "localhost",
            "--daikin-client-id",
            "test-client",
            "--daikin-client-secret",
            "test-secret",
            "--daikin-redirect-uri",
            "https://example.com/callback",
        ])
        .expect("valid custom redirect URI");

        assert_eq!(opt.cloud_redirect_uri, "https://example.com/callback");
    }

    #[test_log::test]
    fn requires_oauth_credentials() {
        for credential in ["--daikin-client-id", "--daikin-client-secret"] {
            let err = parse_args(&["daikin2mqtt", "--mqtt-addr", "localhost", credential, "test-value"])
                .err()
                .expect("the other OAuth credential is required");

            assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
        }
    }

    #[test_log::test]
    fn configures_oauth_environment_and_hides_secret_values() {
        let command = Opt::command();
        for (id, env) in [
            ("cloud_client_id", "DAIKIN_OAUTH_CLIENT_ID"),
            ("cloud_client_secret", "DAIKIN_OAUTH_CLIENT_SECRET"),
            ("cloud_redirect_uri", "DAIKIN_REDIRECT_URI"),
            ("mqtt_password", "DAIKIN_MQTT_PASS"),
        ] {
            let arg = command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .expect("OAuth argument exists");
            assert_eq!(arg.get_env(), Some(std::ffi::OsStr::new(env)));
            if id == "cloud_client_secret" || id == "mqtt_password" {
                assert!(arg.is_hide_env_values_set());
            }
        }
    }
}
