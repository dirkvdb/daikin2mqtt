use serde::Serialize;

#[derive(Serialize)]
pub struct Origin {
    name: String,
    sw: String,
    url: String,
}

impl Origin {
    pub fn duco2mqtt() -> Origin {
        Origin {
            name: String::from(env!("CARGO_PKG_NAME")),
            sw: String::from(env!("CARGO_PKG_VERSION")),
            url: String::from("https://github.com/dirkvdb/duco2mqtt"),
        }
    }
}

#[derive(Serialize)]
pub struct Sensor {
    pub origin: Origin,
    pub name: String,
    pub obj_id: String,
    pub unique_id: String,
    pub stat_t: String,
    pub avty_t: String,
    pub state_class: Option<String>,
    pub unit_of_measurement: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
}

#[derive(Serialize)]
pub struct Number {
    pub origin: Origin,
    pub name: String,
    pub obj_id: String,
    pub unique_id: String,
    pub stat_t: String,
    pub avty_t: String,
    pub cmd_t: String,
    pub device_class: Option<String>,
    pub min: Option<i32>,
    pub max: Option<i32>,
    pub unit_of_measurement: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
}

#[derive(Serialize)]
pub struct Select {
    pub origin: Origin,
    pub name: String,
    pub obj_id: String,
    pub unique_id: String,
    pub stat_t: String,
    pub avty_t: String,
    pub cmd_t: String,
    pub options: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
}

pub fn create_sensor_for_register<TReg: std::string::ToString>(
    node_nr: u16,
    base_topic: &str,
    topic_name: &str,
    reg: TReg,
) -> Sensor {
    let unique_id = format!("duco_node_{}_{}", node_nr, reg.to_string());

    Sensor {
        origin: Origin::duco2mqtt(),
        name: String::from(topic_name),
        obj_id: unique_id.clone(),
        unique_id,
        stat_t: format!("{}node_{}/{}", base_topic, node_nr, topic_name),
        avty_t: format!("{}state", base_topic),
        state_class: None,
        unit_of_measurement: None,
        icon: None,
    }
}

pub fn create_number_for_register<TReg: std::string::ToString>(
    node_nr: u16,
    base_topic: &str,
    topic_name: &str,
    reg: TReg,
) -> Number {
    let unique_id = format!("duco_node_{}_{}", node_nr, reg.to_string());

    Number {
        origin: Origin::duco2mqtt(),
        name: String::from(topic_name),
        obj_id: unique_id.clone(),
        unique_id,
        stat_t: format!("{}node_{}/{}", base_topic, node_nr, topic_name),
        avty_t: format!("{}state", base_topic),
        cmd_t: format!("{}node_{}/cmnd/{}", base_topic, node_nr, topic_name),
        device_class: None,
        min: None,
        max: None,
        unit_of_measurement: None,
        icon: None,
    }
}

pub fn create_select_for_register<TEnum: strum::VariantNames, TReg: std::string::ToString>(
    node_nr: u16,
    base_topic: &str,
    topic_name: &str,
    reg: TReg,
) -> Select {
    let unique_id = format!("duco_node_{}_{}", node_nr, reg.to_string());

    Select {
        origin: Origin::duco2mqtt(),
        name: String::from(topic_name),
        obj_id: unique_id.clone(),
        unique_id,
        stat_t: format!("{}node_{}/{}", base_topic, node_nr, topic_name),
        avty_t: format!("{}state", base_topic),
        cmd_t: format!("{}node_{}/cmnd/{}", base_topic, node_nr, topic_name),
        options: TEnum::VARIANTS.iter().map(|x| x.to_string()).collect(),
        icon: None,
    }
}
