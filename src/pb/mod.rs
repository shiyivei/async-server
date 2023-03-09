//! 指令集模块
//! 在Rust中一种类型到另一种类型的转换先写为v1.into,然后实现 impl From<V1> for V2

mod abi;
use super::command_request::RequestData;
use crate::error::*;
pub use abi::*;

use bytes::Bytes;
use bytes::BytesMut;
use http::StatusCode; // 使用状态码
use std::str;

// 类型的生成，两种方式，一种是直接创建，另一种是由其他类型转换而来
impl CommandRequest {
    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(Hset {
                table: table.into(),
                pair: Some(Kvpair::new(key, value)),
            })),
        }
    }
    /// 创建 HGET 命令,代表了一种可以转为字String的类型
    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hget(Hget {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    /// 创建 HGETALL 命令，这种1
    pub fn new_hgetall(table: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hgetall(Hgetall {
                table: table.into(),
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
        Value {
            value: Some(value::Value::String(s)),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value {
            value: Some(value::Value::String(s.into())), // &str 可以通过into() 转为 String, 反之亦然
        }
    }
}

// from的过程，其实也是值提取的过程
impl From<Value> for CommandResponse {
    fn from(v: Value) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values: vec![v], //把值放在数组中
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut result = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _,
            message: e.to_string(),
            values: vec![],
            pairs: vec![],
        };

        match e {
            KvError::NotFound(_, _) => result.status = StatusCode::NOT_FOUND.as_u16() as _,
            KvError::InvalidCommand(_) => result.status = StatusCode::BAD_REQUEST.as_u16() as _,
            _ => {}
        }

        result
    }
}

/// 从 Vec<Kvpair> 转换成 CommandResponse
impl From<Vec<Kvpair>> for CommandResponse {
    fn from(v: Vec<Kvpair>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            pairs: v,
            ..Default::default()
        }
    }
}

/// 从 i64转换成 Value
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
        let s = str::from_utf8(data).unwrap();
        Ok(Self {
            value: Some(value::Value::String(s.to_string())),
        })
    }
}

// 只把String转为Vec<u8>
impl TryFrom<Value> for Vec<u8> {
    type Error = KvError;
    fn try_from(v: Value) -> Result<Self, Self::Error> {
        let s = match v.value {
            Some(value::Value::String(s)) => s,
            _ => "".to_string(),
        };
        let s: Vec<u8> = s.as_bytes().to_vec();
        Ok(s)
    }
}
