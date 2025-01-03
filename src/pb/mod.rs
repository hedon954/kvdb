mod abi;

pub use abi::{command_request::RequestData, *};
use bytes::Bytes;
use http::StatusCode;
use prost::Message;

use crate::KvError;

impl CommandRequest {
    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(Hset {
                table: table.into(),
                pair: Some(Kvpair::new(key, value)),
            })),
        }
    }

    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hget(Hget {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hgetall(table: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hgetall(Hgetall {
                table: table.into(),
            })),
        }
    }

    pub fn new_publish(topic: impl Into<String>, values: Vec<Value>) -> Self {
        Self {
            request_data: Some(RequestData::Publish(Publish {
                topic: topic.into(),
                values,
            })),
        }
    }

    pub fn new_subscribe(topic: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Subscribe(Subscribe {
                topic: topic.into(),
            })),
        }
    }

    pub fn new_unsubscribe(topic: impl Into<String>, id: u32) -> Self {
        Self {
            request_data: Some(RequestData::Unsubscribe(Unsubscribe {
                topic: topic.into(),
                id,
            })),
        }
    }
}

impl Kvpair {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s)),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.to_string())),
        }
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self {
            value: Some(value::Value::Integer(i)),
        }
    }
}

impl TryFrom<&[u8]> for Value {
    type Error = KvError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(Value::decode(data)?)
    }
}

impl From<Value> for CommandResponse {
    fn from(value: Value) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as u32,
            values: vec![value],
            ..Default::default()
        }
    }
}

impl From<Vec<Kvpair>> for CommandResponse {
    fn from(v: Vec<Kvpair>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as u32,
            pairs: v,
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut res = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
            message: e.to_string(),
            values: vec![],
            pairs: vec![],
        };

        match e {
            KvError::NotFound(_) => res.status = StatusCode::NOT_FOUND.as_u16() as u32,
            KvError::InvalidCommand(_) => res.status = StatusCode::BAD_REQUEST.as_u16() as u32,
            KvError::ConvertCommand(_, _) => res.status = StatusCode::BAD_REQUEST.as_u16() as u32,
            _ => (),
        }
        res
    }
}

impl From<(String, Value)> for Kvpair {
    fn from(kv: (String, Value)) -> Self {
        Kvpair::new(kv.0, kv.1)
    }
}

impl<const N: usize> From<[u8; N]> for Value {
    fn from(data: [u8; N]) -> Self {
        Bytes::copy_from_slice(&data[..]).into()
    }
}

impl From<Bytes> for Value {
    fn from(data: Bytes) -> Self {
        Self {
            value: Some(value::Value::Binary(data)),
        }
    }
}

impl TryFrom<&CommandResponse> for i64 {
    type Error = KvError;

    fn try_from(res: &CommandResponse) -> Result<Self, Self::Error> {
        if res.status != StatusCode::OK.as_u16() as u32 {
            return Err(KvError::ConvertCommand(res.format(), "CommandResponse"));
        }
        match res.values.first() {
            Some(v) => v.try_into(),
            None => Err(KvError::ConvertCommand(res.format(), "CommandResponse")),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = KvError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v.value {
            Some(value::Value::Integer(i)) => Ok(i),
            _ => Err(KvError::ConvertCommand(v.format(), "Integer")),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = KvError;

    fn try_from(v: &Value) -> Result<Self, Self::Error> {
        match v.value {
            Some(value::Value::Integer(i)) => Ok(i),
            _ => Err(KvError::ConvertCommand(v.format(), "Integer")),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = KvError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v.value {
            Some(value::Value::Float(f)) => Ok(f),
            _ => Err(KvError::ConvertCommand(v.format(), "Float")),
        }
    }
}

impl TryFrom<Value> for Bytes {
    type Error = KvError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v.value {
            Some(value::Value::Binary(b)) => Ok(b),
            _ => Err(KvError::ConvertCommand(v.format(), "Binary")),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = KvError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.value {
            Some(value::Value::Bool(b)) => Ok(b),
            _ => Err(KvError::ConvertCommand(value.format(), "Boolean")),
        }
    }
}

impl From<Vec<Value>> for CommandResponse {
    fn from(values: Vec<Value>) -> Self {
        Self {
            values,
            ..Default::default()
        }
    }
}

impl CommandRequest {
    pub fn format(&self) -> String {
        format!("{:?}", self)
    }
}

impl CommandResponse {
    pub fn format(&self) -> String {
        format!("{:?}", self)
    }

    pub fn ok() -> Self {
        Self {
            status: StatusCode::OK.as_u16() as u32,
            ..Default::default()
        }
    }
}

impl Value {
    pub fn format(&self) -> String {
        format!("{:?}", self)
    }
}
