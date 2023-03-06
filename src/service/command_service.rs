//! 根据请求内容分发执行具体的针对数据库的操作
//! 具体操作定义在storage trait中
//!
use crate::error::*;
use crate::pb::*;
use crate::storage::*;
pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

// 为指令实现trait中定义的方法

// 根据数据表和key，返回数据
// 一个trait，多种类型实现，这些类型都能使用同名方法，但是实际内容却不一样
impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}
