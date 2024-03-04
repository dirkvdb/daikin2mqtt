use std::time::Duration;

use rumqttc::v5::{
    mqttbytes::{
        v5::{ConnectReturnCode, LastWill, Packet},
        QoS,
    },
    AsyncClient, Event, EventLoop, MqttOptions,
};

use crate::MqttBridgeError;

#[derive(Clone)]
pub struct MqttConfig {
    pub server: String,
    pub port: u16,
    pub client_id: String,
    pub user: String,
    pub password: String,
    pub base_topic: String,
}

#[derive(Debug)]
pub struct MqttData {
    pub topic: String,
    pub payload: String,
}

pub struct MqttConnection {
    client: AsyncClient,
    eventloop: EventLoop,
    base_topic: String,
}

fn from_mqtt_string(stream: &bytes::Bytes) -> Result<String, MqttBridgeError> {
    match String::from_utf8(stream.to_vec()) {
        Ok(v) => Ok(v),
        Err(_e) => Err(MqttBridgeError::ParseError),
    }
}

const OFFLINE_PAYLOAD: &str = "offline";
const ONLINE_PAYLOAD: &str = "online";

fn state_topic(base_topic: &String) -> String {
    format!("{}/state", base_topic)
}

impl MqttConnection {
    pub fn new(cfg: MqttConfig) -> MqttConnection {
        let mut mqttoptions = MqttOptions::new(cfg.client_id, cfg.server, cfg.port);
        mqttoptions.set_clean_start(true);
        mqttoptions.set_keep_alive(Duration::from_secs(180));
        mqttoptions.set_last_will(LastWill::new(
            state_topic(&cfg.base_topic),
            OFFLINE_PAYLOAD,
            QoS::AtLeastOnce,
            true,
            None,
        ));

        if !cfg.user.is_empty() {
            mqttoptions.set_credentials(cfg.user, cfg.password);
        }

        let (client, eventloop) = AsyncClient::new(mqttoptions, 100);

        log::info!("MQTT connection created");
        MqttConnection {
            client,
            eventloop,
            base_topic: cfg.base_topic,
        }
    }

    pub async fn poll(&mut self) -> Result<Option<MqttData>, MqttBridgeError> {
        if let Ok(msg) = self.eventloop.poll().await {
            return self.handle_mqtt_message(msg).await;
        }

        Ok(None)
    }

    pub async fn publish(&mut self, data: MqttData) -> Result<(), MqttBridgeError> {
        Ok(self
            .client
            .publish(data.topic, QoS::AtLeastOnce, true, data.payload)
            .await?)
    }

    async fn subscribe_to_commands(&mut self) -> Result<(), MqttBridgeError> {
        let cmd_subscription_topic = format!("{}/+/cmnd/+", self.base_topic);
        self.client.subscribe(cmd_subscription_topic, QoS::ExactlyOnce).await?;
        Ok(())
    }

    pub async fn publish_online(&mut self) -> Result<(), MqttBridgeError> {
        self.client
            .publish(state_topic(&self.base_topic), QoS::AtLeastOnce, true, ONLINE_PAYLOAD)
            .await?;

        Ok(())
    }

    pub async fn publish_offline(&mut self) -> Result<(), MqttBridgeError> {
        self.client
            .publish(state_topic(&self.base_topic), QoS::AtLeastOnce, true, OFFLINE_PAYLOAD)
            .await?;

        Ok(())
    }

    async fn handle_mqtt_message(&mut self, ev: Event) -> Result<Option<MqttData>, MqttBridgeError> {
        match ev {
            Event::Incoming(event) => match event {
                Packet::ConnAck(data) => {
                    if data.code == ConnectReturnCode::Success {
                        if !data.session_present {
                            log::info!("Subscribe to mqtt commands");
                            self.subscribe_to_commands().await?;
                        } else {
                            log::debug!("Session still active, no need to resubsribe");
                        }
                    } else {
                        log::error!("MQTT connection refused: {:?}", data.code);
                    }
                }
                Packet::Publish(publ) => {
                    return Ok(Some(MqttData {
                        topic: from_mqtt_string(&publ.topic)?,
                        payload: from_mqtt_string(&publ.payload)?,
                    }));
                }
                //Packet::Connect(_) => todo!(),
                // Packet::PubAck(_) => todo!(),
                // Packet::PubRec(_) => todo!(),
                // Packet::PubRel(_) => todo!(),
                // Packet::PubComp(_) => todo!(),
                // Packet::Subscribe(_) => todo!(),
                // Packet::SubAck(_) => todo!(),
                // Packet::Unsubscribe(_) => todo!(),
                // Packet::UnsubAck(_) => todo!(),
                // Packet::PingReq => todo!(),
                // Packet::PingResp => todo!(),
                // Packet::Disconnect => todo!(),
                _ => {}
            },
            _ => {}
        }

        Ok(None)
    }
}
