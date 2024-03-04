use crate::daikincloudconnection::DaikinCloudConnection;
use crate::mqtt::{MqttConfig, MqttConnection};
use crate::MqttBridgeError;
use log;
use tokio::time;

//const HASS_DISCOVERY_TOPIC: &str = "homeassistant";

pub struct DaikinMqttBridgeConfig {
    pub mqtt_config: MqttConfig,
    pub hass_discovery: bool,
    pub cloud_client_code: String,
    pub poll_interval: time::Duration,
    pub data_dir: std::path::PathBuf,
}

pub struct DaikinMqttBridge {
    mqtt: MqttConnection,
    cloud_client_code: String,
    poll_interval: time::Duration,
    data_dir: std::path::PathBuf,
}

impl DaikinMqttBridge {
    pub fn new(cfg: DaikinMqttBridgeConfig) -> DaikinMqttBridge {
        DaikinMqttBridge {
            mqtt: MqttConnection::new(cfg.mqtt_config),
            cloud_client_code: cfg.cloud_client_code,
            poll_interval: cfg.poll_interval,
            data_dir: cfg.data_dir,
        }
    }

    pub async fn run(mut self) -> Result<(), MqttBridgeError> {
        let mut interval = time::interval(self.poll_interval);

        let client_id = "emU20GdJDiiUxI_HnFGz69dD".to_string();
        let client_secret =
            "TNL1ePwnOkf6o2gKiI8InS8nVwTz2G__VYkv6WznzJGUnwLHLTmKYp-7RZc6FA3yS6D0Wgj_snvqsU5H_LPHQA".to_string();

        let mut connection =
            DaikinCloudConnection::new(client_id, client_secret, self.cloud_client_code, &self.data_dir);

        loop {
            tokio::select! {
                mqtt_msg = self.mqtt.poll() => {
                    if let Ok(Some(msg)) = mqtt_msg {
                        log::debug!("MQTT cmnd: {} {}", msg.topic, msg.payload);
                        // if let Err(err) = self.handle_node_command(&msg).await {
                        //     log::error!("Failed to process command: {}: {} ({err})", msg.topic, msg.payload);
                        // }
                    }
                }
                _ = interval.tick() => {
                    log::info!("Poll cloud");
                    connection.update().await?;
                }
            }
        }
    }
}

// Test for parsing node topics
// #[cfg(test)]
// mod tests {
//     use super::*;
// }
