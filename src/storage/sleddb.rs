use super::Storage;
use crate::error::KvError;
use crate::pb::Kvpair;
use crate::pb::Value;
use crate::StorageIter;
use std::path::Path;
use std::result;
use std::str;

use sled::Db;
use sled::IVec;
#[derive(Debug)]

// 包裹第三方类型
pub struct SledDb(Db);

impl SledDb {
    // 通过路径拿到 Db
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap()) // result 可以用map 和 unwrap取出值
    }

    // 用 prefix 模拟table
    pub fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }

    pub fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}

// 辅助函数， 反转类型
fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some)) // Option 也有一系列map方法和unwrap方法
}

// 更换底层的数据结构只需要实现trait定义好的接口即可

impl Storage for SledDb {
    // 从表里取数据
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, &key);
        let result = self.0.get(name.as_bytes())?.map(|v| v.as_ref().try_into());
        flip(result)
    }
    // 向表里存数据
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, &key);

        let data: Vec<u8> = value.try_into()?;
        let result = self.0.insert(name, data)?.map(|v| v.as_ref().try_into());

        flip(result)
    } // 返回前值
      // 判断存在性
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let name = SledDb::get_full_key(table, &key);
        Ok(self.0.contains_key(name)?)
    }
    // 删除数据
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, &key);
        let result = self.0.remove(name)?.map(|v| v.as_ref().try_into());
        flip(result)
    }
    // 删除表
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let prefix = SledDb::get_table_prefix(table);

        let result = self.0.scan_prefix(table).map(|v| v.into()).collect();
        Ok(result)
    }
    // 把数据转为迭代器，方便遍历，值有多种类型，但是都会实现迭代器trait,并且类型是Kvpair
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let prefix = SledDb::get_table_prefix(table);

        let iter = StorageIter::new(self.0.scan_prefix(prefix));

        Ok(Box::new(iter))
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for Kvpair {
    fn from(value: Result<(IVec, IVec), sled::Error>) -> Self {
        match value {
            Ok((k, v)) => match v.as_ref().try_into() {
                Ok(v) => Kvpair::new(ivec_to_key(k.as_ref()), v),
                Err(_) => Kvpair::default(),
            },
            _ => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> &str {
    let s = str::from_utf8(ivec).unwrap();
    let mut iter = s.split(":");
    iter.next();
    iter.next().unwrap()
}
