use super::*;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use bytes::BytesMut;
use rumqttc::v5::mqttbytes::{v5::Packet, Error as MqttError};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};

const BASE: &str = "test/command-refresh";
const QUIET: time::Duration = time::Duration::from_millis(2200);

type CloudState = Arc<Mutex<MockCloud>>;

struct MockCloud {
    snapshot: Value,
    gets: usize,
    patches: usize,
    fail_get: bool,
    reject_patch: Option<usize>,
    hold_patch: Option<(usize, Arc<Notify>)>,
    events: mpsc::UnboundedSender<CloudEvent>,
}

#[derive(Debug)]
enum CloudEvent {
    Get(time::Instant),
    Patch(String, Value),
    PatchFinished(time::Instant, StatusCode),
}

async fn get_devices(State(state): State<CloudState>) -> impl IntoResponse {
    let mut cloud = state.lock().await;
    cloud.gets += 1;
    cloud.events.send(CloudEvent::Get(time::Instant::now())).expect("record mock cloud GET");
    let status = if cloud.fail_get { StatusCode::BAD_GATEWAY } else { StatusCode::OK };
    (status, Json(cloud.snapshot.clone()))
}

async fn patch_device(State(state): State<CloudState>, uri: axum::http::Uri, Json(body): Json<Value>) -> StatusCode {
    let (gate, status, events) = {
        let mut cloud = state.lock().await;
        cloud.patches += 1;
        cloud
            .events
            .send(CloudEvent::Patch(uri.path().to_owned(), body))
            .expect("record mock cloud PATCH");
        let gate = cloud
            .hold_patch
            .as_ref()
            .filter(|(index, _)| *index == cloud.patches)
            .map(|(_, gate)| gate.clone());
        let status = if cloud.reject_patch == Some(cloud.patches) {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::NO_CONTENT
        };
        (gate, status, cloud.events.clone())
    };
    if let Some(gate) = gate {
        gate.notified().await;
    }
    events
        .send(CloudEvent::PatchFinished(time::Instant::now(), status))
        .expect("record mock cloud PATCH response");
    status
}

// Abort even when an assertion or the outer timeout unwinds a test.
#[derive(Default)]
struct Tasks(Vec<JoinHandle<()>>);

impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

struct Harness {
    cloud: CloudState,
    events: mpsc::UnboundedReceiver<CloudEvent>,
    commands: mpsc::UnboundedSender<MqttData>,
    published: mpsc::UnboundedReceiver<MqttData>,
    _tasks: Tasks,
    _data_dir: tempfile::TempDir,
}

fn fixture() -> Value {
    let mut devices: Value = serde_json::from_str(include_str!("../../resources/test/managementpoints.json")).expect("parse device fixture");
    let point = climate_point(&mut devices);
    point["operationMode"]["settable"] = json!(true);
    point["operationMode"]["values"] = json!(["heating", "auto"]);
    devices
}

fn climate_point(devices: &mut Value) -> &mut Value {
    devices[0]["managementPoints"]
        .as_array_mut()
        .expect("device fixture management points array")
        .iter_mut()
        .find(|point| point["embeddedId"] == "climateControlMainZone")
        .expect("climate control management point in device fixture")
}

async fn read_packet(stream: &mut TcpStream, buffer: &mut BytesMut) -> Packet {
    loop {
        match Packet::read(buffer, None) {
            Ok(packet) => return packet,
            Err(MqttError::InsufficientBytes(_)) => assert!(stream.read_buf(buffer).await.expect("read MQTT packet") > 0),
            Err(err) => panic!("invalid MQTT packet: {err}"),
        }
    }
}

async fn broker(listener: TcpListener, mut commands: mpsc::UnboundedReceiver<MqttData>, published: mpsc::UnboundedSender<MqttData>) {
    let (mut stream, _) = listener.accept().await.expect("accept test MQTT connection");
    stream.set_nodelay(true).expect("disable test MQTT socket buffering");
    let mut buffer = BytesMut::new();
    assert!(matches!(read_packet(&mut stream, &mut buffer).await, Packet::Connect(..)));
    stream.write_all(&[0x20, 0x03, 0, 0, 0]).await.expect("send MQTT CONNACK");
    loop {
        tokio::select! {
            packet = read_packet(&mut stream, &mut buffer) => match packet {
                Packet::Subscribe(subscribe) => {
                    let id = subscribe.pkid.to_be_bytes();
                    stream.write_all(&[0x90, 0x04, id[0], id[1], 0, 2]).await.expect("send MQTT SUBACK");
                }
                Packet::Publish(publish) => {
                    assert!(publish.retain, "confirmed status must be retained");
                    let id = publish.pkid.to_be_bytes();
                    stream.write_all(&[0x40, 0x02, id[0], id[1]]).await.expect("send MQTT PUBACK");
                    published.send(MqttData::new(
                        String::from_utf8(publish.topic.to_vec()).expect("decode published MQTT topic"),
                        String::from_utf8(publish.payload.to_vec()).expect("decode published MQTT payload"),
                    )).expect("record published MQTT status");
                }
                Packet::PingReq(_) => stream.write_all(&[0xd0, 0]).await.expect("send MQTT PINGRESP"),
                _ => {}
            },
            Some(command) = commands.recv() => {
                // MQTT 5 QoS 0 PUBLISH, no properties; these short packets fit one length byte.
                let length = 2 + command.topic.len() + 1 + command.payload.len();
                assert!(length < 128);
                let mut packet = vec![0x30, length as u8];
                packet.extend_from_slice(&(command.topic.len() as u16).to_be_bytes());
                packet.extend_from_slice(command.topic.as_bytes());
                packet.push(0);
                packet.extend_from_slice(command.payload.as_bytes());
                stream.write_all(&packet).await.expect("send live MQTT command");
            }
        }
    }
}

impl Harness {
    async fn start(poll_interval: time::Duration) -> Self {
        let mut tasks = Tasks::default();
        let data_dir = tempfile::tempdir().expect("create test authentication directory");
        std::fs::write(
            data_dir.path().join("openid_authentication.json"),
            json!({
                "token": { "access_token": "local-test-token", "refresh_token": null, "expires_at": u64::MAX },
            })
            .to_string(),
        )
        .expect("seed test authentication token");
        let (events_tx, events) = mpsc::unbounded_channel();
        let cloud = Arc::new(Mutex::new(MockCloud {
            snapshot: fixture(),
            gets: 0,
            patches: 0,
            fail_get: false,
            reject_patch: None,
            hold_patch: None,
            events: events_tx,
        }));
        let http_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock cloud HTTP server");
        let api = format!("http://{}/", http_listener.local_addr().expect("mock cloud HTTP address"));
        let router = Router::new()
            .route("/gateway-devices", get(get_devices))
            .route(
                "/gateway-devices/{gateway}/management-points/{point}/characteristics/{characteristic}",
                axum::routing::patch(patch_device),
            )
            .with_state(cloud.clone());
        tasks.0.push(tokio::spawn(async move {
            axum::serve(http_listener, router).await.expect("serve mock cloud HTTP requests");
        }));
        let mqtt_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test MQTT broker");
        let port = mqtt_listener.local_addr().expect("test MQTT broker address").port();
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let (published_tx, published) = mpsc::unbounded_channel();
        tasks.0.push(tokio::spawn(broker(mqtt_listener, commands_rx, published_tx)));
        let mut bridge = DaikinMqttBridge::new(DaikinMqttBridgeConfig {
            mqtt_config: MqttConfig {
                server: "127.0.0.1".into(),
                port,
                client_id: "command-refresh-test".into(),
                user: String::new(),
                password: String::new(),
                base_topic: BASE.into(),
            },
            cloud_client_id: "test-client".into(),
            cloud_client_secret: "test-secret".into(),
            cloud_redirect_uri: "http://localhost:8076/oauth/callback".into(),
            web_listen: "127.0.0.1:0".parse().expect("parse test web listen address"),
            hass_discovery: false,
            poll_interval,
            data_dir: data_dir.path().to_owned(),
        });
        let mut connection = DaikinCloudConnection::new(
            "test-client".into(),
            "test-secret".into(),
            "http://localhost:8076/oauth/callback".into(),
            data_dir.path(),
        )
        .expect("create test cloud connection");
        connection.set_test_api_endpoint(api);
        // Establish MQTT before the immediate initial poll can cancel the connection handshake.
        bridge.mqtt.poll().await.expect("connect test MQTT client");
        tasks.0.push(tokio::spawn(async move {
            bridge
                .run_bridge(Arc::new(Mutex::new(connection)), Arc::new(Notify::new()))
                .await
                .expect("run test bridge event loop");
        }));
        let mut harness = Self {
            cloud,
            events,
            commands,
            published,
            _tasks: tasks,
            _data_dir: data_dir,
        };
        harness.next_get().await;
        let initial = harness.snapshot().await;
        assert_eq!(initial["heating_setpoint_room_temperature"], 19.0);
        harness
    }

    fn command(&self, kind: &str, payload: &str) {
        self.commands
            .send(MqttData::new(commands::topic(BASE, kind), payload.into()))
            .expect("queue test MQTT command");
    }

    async fn next_get(&mut self) -> time::Instant {
        match self.events.recv().await.expect("receive mock cloud GET event") {
            CloudEvent::Get(at) => at,
            event => panic!("expected GET, got {event:?}"),
        }
    }

    async fn command_get(&mut self, accepted: time::Instant) {
        let delay = time::Duration::from_secs(2);
        let read = time::timeout_at(accepted + delay + time::Duration::from_millis(500), self.next_get())
            .await
            .expect("one refresh promptly after the two-second delay");
        assert!(read >= accepted + delay, "read before two seconds after final PATCH acceptance");
    }

    async fn patch(&mut self, characteristic: &str, value: Value) {
        match self.events.recv().await.expect("receive mock cloud PATCH event") {
            CloudEvent::Patch(path, body) => {
                assert!(path.ends_with(&format!("/characteristics/{characteristic}")), "{path}");
                assert_eq!(body["value"], value);
                if characteristic == "temperatureControl" {
                    assert_eq!(body["path"], "/operationModes/heating/setpoints/roomTemperature");
                }
            }
            event => panic!("expected PATCH, got {event:?}"),
        }
    }

    async fn finished(&mut self, expected: StatusCode) -> time::Instant {
        match self.events.recv().await.expect("receive mock cloud PATCH response event") {
            CloudEvent::PatchFinished(at, status) => {
                assert_eq!(status, expected);
                at
            }
            event => panic!("expected PATCH response, got {event:?}"),
        }
    }

    async fn temperature(&mut self, value: u8) -> time::Instant {
        self.command(commands::CLIMATE_TEMPERATURE, &value.to_string());
        self.patch("temperatureControl", json!(value as f64)).await;
        self.finished(StatusCode::NO_CONTENT).await
    }

    async fn snapshot(&mut self) -> Value {
        let mut climate = None;
        loop {
            let message = self.published.recv().await.expect("receive published MQTT snapshot");
            if message.topic == format!("{BASE}/climate_control") {
                climate = Some(serde_json::from_str(&message.payload).expect("parse published climate snapshot"));
            } else if message.topic == format!("{BASE}/domestic_hot_water") {
                return climate.expect("climate snapshot precedes hot water snapshot");
            }
        }
    }

    async fn assert_quiet(&mut self, gets: usize, duration: time::Duration) {
        time::sleep(duration).await;
        assert!(self._tasks.0.iter().all(|task| !task.is_finished()), "mock or bridge task stopped");
        assert_eq!(self.cloud.lock().await.gets, gets, "unexpected cloud read/retry");
        assert!(self.events.try_recv().is_err(), "unexpected cloud request");
        assert!(self.published.try_recv().is_err(), "unexpected/optimistic MQTT status");
    }
}

#[tokio::test]
async fn rapid_increases_reset_debounce_and_publish_one_confirmed_snapshot() {
    time::timeout(time::Duration::from_secs(10), async {
        // The ordinary three-second poll becomes due during the debounce window.
        let mut h = Harness::start(time::Duration::from_secs(3)).await;
        h.temperature(20).await;
        time::sleep(time::Duration::from_millis(900)).await;
        h.temperature(21).await;
        time::sleep(time::Duration::from_millis(900)).await;
        let accepted = h.temperature(22).await;
        // Deliberately differs from the requested value: MQTT must report the GET, not an optimistic target.
        climate_point(&mut h.cloud.lock().await.snapshot)["temperatureControl"]["value"]["operationModes"]["heating"]["setpoints"]["roomTemperature"]
            ["value"] = json!(22.5);
        time::sleep_until(accepted + time::Duration::from_millis(1900)).await;
        h.assert_quiet(1, time::Duration::ZERO).await;
        h.command_get(accepted).await;
        assert_eq!(h.snapshot().await["heating_setpoint_room_temperature"], 22.5);
        assert_eq!(h.cloud.lock().await.patches, 3);
        // Beyond both a retry delay and the old regular timer's next tick, but before the reset interval.
        h.assert_quiet(2, time::Duration::from_millis(2500)).await;
    })
    .await
    .expect("debounced event-loop refresh completes within ten seconds");
}

#[tokio::test]
async fn stale_and_failed_refreshes_are_consumed_without_retries() {
    time::timeout(time::Duration::from_secs(12), async {
        let mut h = Harness::start(time::Duration::from_secs(60)).await;
        for (index, fail) in [false, true].into_iter().enumerate() {
            h.cloud.lock().await.fail_get = fail;
            let accepted = h.temperature(20 + index as u8).await;
            h.command_get(accepted).await;
            if fail {
                let message = h.published.recv().await.expect("receive MQTT offline status after failed GET");
                assert_eq!(message.topic, format!("{BASE}/state"));
                assert_eq!(message.payload, "offline");
            } else {
                // The cloud is still stale; publish that snapshot once, without matching/retry loops.
                assert_eq!(h.snapshot().await["heating_setpoint_room_temperature"], 19.0);
            }
            h.assert_quiet(index + 2, QUIET).await;
        }
    })
    .await
    .expect("stale and failed GET scenarios complete within twelve seconds");
}

#[tokio::test]
async fn invalid_rejected_and_partially_rejected_commands_create_no_refresh() {
    time::timeout(time::Duration::from_secs(8), async {
        let mut h = Harness::start(time::Duration::from_secs(60)).await;
        h.cloud.lock().await.reject_patch = Some(1);
        h.command(commands::CLIMATE_TEMPERATURE, "not-a-number");
        h.command(commands::CLIMATE_TEMPERATURE, "999");
        h.command("unknown", "on");
        h.command(commands::CLIMATE_TEMPERATURE, "20");
        h.patch("temperatureControl", json!(20.0)).await;
        h.finished(StatusCode::BAD_REQUEST).await;
        h.assert_quiet(1, QUIET).await;
        assert_eq!(h.cloud.lock().await.patches, 1, "invalid commands must not reach HTTP");

        h.cloud.lock().await.reject_patch = Some(3);
        h.command(commands::CLIMATE_MODE, "auto");
        h.patch("operationMode", json!("auto")).await;
        h.finished(StatusCode::NO_CONTENT).await;
        h.patch("onOffMode", json!("on")).await;
        h.finished(StatusCode::BAD_REQUEST).await;
        h.assert_quiet(1, QUIET).await;
        assert_eq!(h.cloud.lock().await.patches, 3);
    })
    .await
    .expect("rejected command scenarios complete within eight seconds");
}

#[tokio::test]
async fn multi_write_mode_waits_two_seconds_after_the_final_patch_is_accepted() {
    time::timeout(time::Duration::from_secs(10), async {
        let mut h = Harness::start(time::Duration::from_secs(60)).await;
        let release = Arc::new(Notify::new());
        h.cloud.lock().await.hold_patch = Some((2, release.clone()));
        h.command(commands::CLIMATE_MODE, "auto");
        h.patch("operationMode", json!("auto")).await;
        h.finished(StatusCode::NO_CONTENT).await;
        h.patch("onOffMode", json!("on")).await;
        // The second request is in flight for longer than the entire debounce delay.
        h.assert_quiet(1, QUIET).await;
        {
            let mut cloud = h.cloud.lock().await;
            let point = climate_point(&mut cloud.snapshot);
            point["operationMode"]["value"] = json!("auto");
            point["onOffMode"]["value"] = json!("on");
        }
        release.notify_one();
        let accepted = h.finished(StatusCode::NO_CONTENT).await;
        time::sleep_until(accepted + time::Duration::from_millis(1900)).await;
        h.assert_quiet(1, time::Duration::ZERO).await;
        h.command_get(accepted).await;
        let snapshot = h.snapshot().await;
        assert_eq!(snapshot["operation_mode"], "auto");
        assert_eq!(snapshot["on_off_mode"], "on");
        assert_eq!(h.cloud.lock().await.patches, 2);
        h.assert_quiet(2, QUIET).await;
    })
    .await
    .expect("multi-write refresh completes within ten seconds");
}
