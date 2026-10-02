#![warn(clippy::unwrap_used)]
use core::time;
use std::{collections::HashMap, path::PathBuf};

use clap::Parser;
use daikin2mqtt::{
    bridge::{self, DaikinMqttBridgeConfig},
    mqtt::MqttConfig,
};
use env_logger::Env;
use secretspec::{SecretBytes, SecretSpecError, Secrets, Spec};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const PACKAGE: &str = env!("CARGO_PKG_NAME");

#[derive(Parser)]
#[clap(name = "daikin2mqtt", about = "Interface between daikin heatpump and MQTT")]
struct Opt {
    #[clap(long = "daikin-client-id", env = "DAIKIN_OAUTH_CLIENT_ID")]
    cloud_client_id: String,

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

fn main() -> secretspec::Result<()> {
    let opt = Opt::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();

    log::info!("{} version {}", PACKAGE, VERSION);

    let secrets = secret_spec()?.ensure_secrets(None, None, false)?;
    let cfg = bridge_config(opt, secrets.resolved.secrets)?;

    // Resolve secrets before entering Tokio: providers may use their own runtimes.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async { bridge::DaikinMqttBridge::new(cfg).run().await })
        .expect("Failed to run bridge");
    Ok(())
}

fn secret_spec() -> secretspec::Result<Secrets> {
    let spec = Spec::from_toml(include_str!("../../secretspec.toml"))?;
    let mut secrets = Secrets::from_spec(spec)?.with_default_reason("Initialize the Daikin MQTT bridge");
    secrets.set_ignore_ambient_scope(true);
    Ok(secrets)
}

fn bridge_config(opt: Opt, mut secrets: HashMap<String, SecretBytes>) -> secretspec::Result<DaikinMqttBridgeConfig> {
    let mut take_secret = |name: &str| -> secretspec::Result<String> {
        let value = secrets
            .remove(name)
            .ok_or_else(|| SecretSpecError::RequiredSecretMissing(name.to_owned()))?;
        Ok(value.try_as_utf8_for(name)?.to_owned())
    };

    Ok(DaikinMqttBridgeConfig {
        mqtt_config: MqttConfig {
            server: opt.mqtt_addr,
            port: opt.mqtt_port,
            client_id: opt.mqtt_client_id,
            user: take_secret("MQTT_USERNAME")?,
            password: take_secret("MQTT_PASSWORD")?,
            base_topic: opt.mqtt_base_topic,
        },
        cloud_client_id: opt.cloud_client_id,
        cloud_client_secret: take_secret("DAIKIN_CLIENT_SECRET")?,
        cloud_redirect_uri: opt.cloud_redirect_uri,
        //hass_discovery: opt.hass_discovery,
        data_dir: PathBuf::from(opt.data_dir),
        poll_interval: time::Duration::from_secs(opt.cloud_poll_interval),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{error::ErrorKind, CommandFactory, FromArgMatches};
    use std::io::Write;

    fn parse_args(args: &[&str]) -> Result<Opt, clap::Error> {
        let matches = Opt::command()
            .mut_args(|arg| arg.env(None::<&str>))
            .try_get_matches_from(args)?;
        Opt::from_arg_matches(&matches)
    }

    #[test_log::test]
    fn parses_oauth_client_id_with_default_redirect_uri() {
        let opt = parse_args(&[
            "daikin2mqtt",
            "--mqtt-addr",
            "localhost",
            "--daikin-client-id",
            "test-client",
        ])
        .expect("valid OAuth options without CLI secrets");

        assert_eq!(opt.cloud_client_id, "test-client");
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
            "--daikin-redirect-uri",
            "https://example.com/callback",
        ])
        .expect("valid custom redirect URI");

        assert_eq!(opt.cloud_redirect_uri, "https://example.com/callback");
    }

    #[test_log::test]
    fn requires_oauth_client_id() {
        let err = parse_args(&["daikin2mqtt", "--mqtt-addr", "localhost"])
            .err()
            .expect("the OAuth client ID is required");

        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test_log::test]
    fn configures_non_secret_oauth_environment() {
        let command = Opt::command();
        for (id, env) in [
            ("cloud_client_id", "DAIKIN_OAUTH_CLIENT_ID"),
            ("cloud_redirect_uri", "DAIKIN_REDIRECT_URI"),
        ] {
            let arg = command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .expect("OAuth argument exists");
            assert_eq!(arg.get_env(), Some(std::ffi::OsStr::new(env)));
        }
    }

    #[test_log::test]
    fn secrets_are_not_exposed_as_cli_options() {
        let help = Opt::command().render_long_help().to_string();
        for option in ["--daikin-client-secret", "--mqtt-user", "--mqtt-pass"] {
            assert!(!help.contains(option));
        }
    }

    fn resolve_secrets(contents: &str) -> secretspec::Result<HashMap<String, SecretBytes>> {
        let mut file = tempfile::NamedTempFile::new().expect("create isolated secret file");
        file.write_all(contents.as_bytes()).expect("write test secrets");
        let mut spec = secret_spec()?;
        spec.set_provider(format!("dotenv:{}", file.path().display()));
        spec.set_profile("default");
        Ok(spec.ensure_secrets(None, None, false)?.resolved.secrets)
    }

    #[test_log::test]
    fn loads_embedded_secret_spec_into_bridge_config() {
        let secrets =
            resolve_secrets("DAIKIN_CLIENT_SECRET=test-secret\nMQTT_USERNAME=test-user\nMQTT_PASSWORD=test-pass\n")
                .expect("all required secrets are available");
        let opt = parse_args(&[
            "daikin2mqtt",
            "--mqtt-addr",
            "localhost",
            "--daikin-client-id",
            "test-client",
            "--mqtt-port",
            "1884",
            "--mqtt-client-id",
            "test-mqtt-client",
            "--mqtt-base-topic",
            "home/daikin",
            "--daikin-poll-interval",
            "60",
            "--data-dir",
            "test-data",
        ])
        .expect("valid bridge options");
        let cfg = bridge_config(opt, secrets).expect("valid bridge configuration");

        assert_eq!(cfg.cloud_client_id, "test-client");
        assert_eq!(cfg.cloud_client_secret, "test-secret");
        assert_eq!(cfg.cloud_redirect_uri, "onectaintegration://callback/");
        assert_eq!(cfg.mqtt_config.user, "test-user");
        assert_eq!(cfg.mqtt_config.password, "test-pass");
        assert_eq!(cfg.mqtt_config.server, "localhost");
        assert_eq!(cfg.mqtt_config.port, 1884);
        assert_eq!(cfg.mqtt_config.client_id, "test-mqtt-client");
        assert_eq!(cfg.mqtt_config.base_topic, "home/daikin");
        assert_eq!(cfg.poll_interval, time::Duration::from_secs(60));
        assert_eq!(cfg.data_dir, PathBuf::from("test-data"));
    }

    #[test_log::test]
    fn rejects_each_missing_required_secret_without_exposing_values() {
        let values = [
            ("DAIKIN_CLIENT_SECRET", "test-secret"),
            ("MQTT_USERNAME", "test-user"),
            ("MQTT_PASSWORD", "test-pass"),
        ];
        for (missing, _) in values {
            let contents: String = values
                .iter()
                .filter(|(name, _)| *name != missing)
                .map(|(name, value)| format!("{name}={value}\n"))
                .collect();
            let err = resolve_secrets(&contents).expect_err("a required secret is missing");
            assert!(matches!(err, SecretSpecError::RequiredSecretMissing(_)));
            let message = err.to_string();
            assert!(message.contains(missing));
            for (_, value) in values {
                assert!(!message.contains(value));
            }
        }
    }
}
