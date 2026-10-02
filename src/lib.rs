#![warn(clippy::unwrap_used)]
pub mod bridge;
mod daikincloudconnection;
//mod hassdiscovery;
pub mod mqtt;
mod web;

use std::{array::TryFromSliceError, fmt, net::AddrParseError, str::Utf8Error};

use reqwest::Url;

#[derive(Debug)]
pub enum MqttBridgeError {
    NetworkError(String),
    RuntimeError(String),
    AuthorizationRequired(Url),
    ParseError,
    NotAuhrorized,
}

impl fmt::Display for MqttBridgeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MqttBridgeError::NetworkError(str) => write!(f, "Network Error {}", str),
            MqttBridgeError::RuntimeError(str) => write!(f, "Runtime Error {}", str),
            MqttBridgeError::AuthorizationRequired(url) => {
                write!(f, "Authorization Required: visit {url}")
            }
            MqttBridgeError::ParseError => write!(f, "Parse Error"),
            MqttBridgeError::NotAuhrorized => write!(f, "Authentication Error"),
        }
    }
}

impl From<std::io::Error> for MqttBridgeError {
    fn from(err: std::io::Error) -> Self {
        MqttBridgeError::NetworkError(format!("IO: {}", err))
    }
}

impl From<std::num::ParseIntError> for MqttBridgeError {
    fn from(_: std::num::ParseIntError) -> Self {
        MqttBridgeError::ParseError
    }
}

impl From<String> for MqttBridgeError {
    fn from(str: String) -> Self {
        MqttBridgeError::RuntimeError(str)
    }
}

impl From<AddrParseError> for MqttBridgeError {
    fn from(_: AddrParseError) -> Self {
        MqttBridgeError::NetworkError(String::from("Invalid address"))
    }
}

impl From<TryFromSliceError> for MqttBridgeError {
    fn from(_: TryFromSliceError) -> Self {
        MqttBridgeError::ParseError
    }
}

impl From<Utf8Error> for MqttBridgeError {
    fn from(_: Utf8Error) -> Self {
        MqttBridgeError::ParseError
    }
}

impl From<rumqttc::v5::ClientError> for MqttBridgeError {
    fn from(err: rumqttc::v5::ClientError) -> Self {
        MqttBridgeError::RuntimeError(format!("MQTT Error: {err}"))
    }
}

impl From<rumqttc::v5::ConnectionError> for MqttBridgeError {
    fn from(err: rumqttc::v5::ConnectionError) -> Self {
        MqttBridgeError::RuntimeError(format!("MQTT Connection error: {err}"))
    }
}

impl From<serde_json::Error> for MqttBridgeError {
    fn from(err: serde_json::Error) -> Self {
        MqttBridgeError::RuntimeError(format!("Serde: {}", err))
    }
}
