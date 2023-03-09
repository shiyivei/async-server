//! 定义数据库与指令接口
//!
use crate::error::*;
use crate::pb::*;
use crate::storage::MemTable;
pub trait Storage {
    // 从表里取数据
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    // 向表里存数据
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>; // 返回前值
                                                                                             // 判断存在性
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    // 删除数据
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    // 删除表
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;
    // 把数据转为迭代器，方便遍历，值有多种类型，但是都会实现迭代器trait,并且类型是Kvpair
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

// 使用泛型结构体包裹类型
pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> Iterator for StorageIter<T>
where
    T: Iterator,
    T::Item: Into<Kvpair>,
{
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|v| v.into())
    }
}

// 单元测试
#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    fn memtable_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(store);
    }

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basi_interface(store);
    }

    fn test_basi_interface(store: impl Storage) {
        // 第一次 set 会创建 table，插入 key 并返回 None（之前没值）
        let v = store.set("t1", "k1".into(), "v".into());
        assert!(v.unwrap().is_none());
        // 再次 set 同样的 key 会更新，并返回之前的值
        let v1 = store.set("t1", "k1".into(), "v1".into());
        assert_eq!(v1, Ok(Some("v".into())));
        // get 存在的 key 会得到最新的值
        let v = store.get("t1", "k1");
        assert_eq!(v, Ok(Some("v1".into())));

        // get 不存在的key or 不存在的 table
        assert_eq!(store.get("t1", "k2"), Ok(None)); // 有表无键
        assert!(store.get("t2", "k1").unwrap().is_none()); // 无表
                                                           // contains 纯在的 key 返回 true，否则 false
        assert_eq!(store.contains("t1", "k1"), Ok(true)); // contains 用在map上
        assert_eq!(store.contains("t1", "k2"), Ok(false));
        assert_eq!(store.contains("t2", "k1"), Ok(false));
        // del 存在的 key 返回删掉的值
        let v = store.del("t1", "k1");
        assert_eq!(v, Ok(Some("v1".into())));
        // del 存在的 key 返回之前的值
        let v = store.del("t1", "hello"); // 删除不存在的key
        assert_eq!(v, Ok(None));
        // del 不存在的 key 或 table 返回 None
        assert_eq!(Ok(None), store.del("t1", "k1"));
        assert_eq!(Ok(None), store.del("t2", "k"));
    }

    fn test_get_all(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();

        let mut data = store.get_all("t2").unwrap();
        // 排序
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        );
    }

    fn test_get_iter(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();

        let mut data: Vec<_> = store.get_iter("t2").unwrap().collect();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        );
    }
}
