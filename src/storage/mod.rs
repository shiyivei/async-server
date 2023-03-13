//! 数据库模块，规定类型能够对数据库执行哪些操作
//!

mod memory;
mod sleddb;
mod storage;
use crate::pb::Kvpair;
pub use memory::*;
pub use sleddb::*;
pub use storage::*;

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

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn sleddb_basic_interface_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_basi_interface(store);
    }
    #[test]
    fn sleddb_get_all_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_all(store);
    }
    #[test]
    fn sleddb_iter_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_iter(store);
    }

    fn test_basi_interface(store: impl Storage) {
        // 第一次 set 会创建 table，插入 key 并返回 None（之前没值）
        let v = store.set("t1", "k1".into(), "v".into());
        assert!(v.unwrap().is_none());
        // 再次 set 同样的 key 会更新，并返回之前的值
        let v1 = store.set("t1", "k1".into(), "v1".into());
        assert_eq!(v1.unwrap(), Some("v".into()));
        // get 存在的 key 会得到最新的值
        let v = store.get("t1", "k1");
        assert_eq!(v.unwrap(), Some("v1".into()));

        // get 不存在的key or 不存在的 table
        assert_eq!(store.get("t1", "k2").unwrap(), None); // 有表无键
        assert!(store.get("t2", "k1").unwrap().is_none()); // 无表
                                                           // contains 纯在的 key 返回 true，否则 false
        assert_eq!(store.contains("t1", "k1").unwrap(), true); // contains 用在map上
        assert_eq!(store.contains("t1", "k2").unwrap(), false);
        assert_eq!(store.contains("t2", "k1").unwrap(), false);
        // del 存在的 key 返回删掉的值
        let v = store.del("t1", "k1");
        assert_eq!(v.unwrap(), Some("v1".into()));
        // del 存在的 key 返回之前的值
        let v = store.del("t1", "hello"); // 删除不存在的key
        assert_eq!(v.unwrap(), None);
        // del 不存在的 key 或 table 返回 None
        assert_eq!(None, store.del("t1", "k1").unwrap());
        assert_eq!(None, store.del("t2", "k").unwrap());
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
