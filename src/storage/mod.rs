mod memory;
mod sleddb;

use crate::{KvError, Kvpair, Value};

pub use memory::MemTable;
pub use sleddb::SledDb;

/// Storage is a trait that defines the interface for a key-value storage engine,
/// the backend may be a memory HashMap or other storage engines like sled, rocksdb, etc.
pub trait Storage {
    /// Get the value of a key in a table
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;

    /// Set the value of a key in a table and return the old value
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;

    /// Check if a key exists in a table
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;

    /// Remove a key in a table and return the removed value
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;

    /// Get all keys in a table
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;

    /// Get an iterator of all key-value pairs in a table
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

/// An iterator that converts the item type of the underlying iterator to Kvpair
pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

/// Why do we implement the `Iterator` trait for `StorageIter`?
/// Thinking the following steps for creating a new Storage and how to implement the `get_iter` method:
/// 1. get the specific table's owning iterator;
/// 2. map for the iterator;
/// 3. convert the item type of the iterator to Kvpair;
/// 4. return the new iterator;
///
/// Only the 1st step is specific to the Storage, the other steps are common for all iterators.
/// So we can implement the `Iterator` trait for `StorageIter` to avoid code duplication.
///
/// Example:
///
/// fn get_iter(
///    &self,
///    table: &str,
/// ) -> Result<Box<dyn Iterator<Item = crate::Kvpair>>, crate::KvError> {
///    let table = self.get_or_create_table(table).clone();
///    let iter = StorageIter::new(table.into_iter());
///    Ok(Box::new(iter))
/// }
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

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basic_interface(store);
    }

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    fn memtable_get_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(store);
    }

    fn test_basic_interface(store: impl Storage) {
        // 1. set an unexisting key, should return None
        assert_eq!(Ok(None), store.set("t1", "hello".into(), "value".into()));

        // 2. set an existing key, should return the old value
        assert_eq!(
            Ok(Some("value".into())),
            store.set("t1", "hello".into(), "value2".into())
        );

        // 3. get the key, should return the new value
        assert_eq!(Ok(Some("value2".into())), store.get("t1", "hello"));

        // 4. get the unexisting key or table, should return None
        assert_eq!(Ok(None), store.get("t1", "unexisting"));
        assert_eq!(Ok(None), store.get("unexisting", "hello"));

        // 5. check the existing key, should return true
        assert_eq!(Ok(true), store.contains("t1", "hello"));

        // 6. check the unexisting key or table, should return false
        assert_eq!(Ok(false), store.contains("t1", "unexisting"));
        assert_eq!(Ok(false), store.contains("unexisting", "hello"));

        // 7. del the key, should return the value
        let v = store.del("t1", "hello");
        assert_eq!(v, Ok(Some("value2".into())));

        // 8. get the key, should return None
        assert_eq!(Ok(None), store.get("t1", "hello"));

        // 9. del the unexisting key or table   , should return None
        assert_eq!(Ok(None), store.del("t1", "unexisting"));
        assert_eq!(Ok(None), store.del("unexisting", "hello"));
    }

    fn test_get_all(store: impl Storage) {
        assert_eq!(Ok(vec![]), store.get_all("t2"));

        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();

        let mut data = store.get_all("t2").unwrap();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        );
    }

    #[allow(unused)]
    fn test_get_iter(store: impl Storage) {
        store.set("t3", "k1".into(), "v1".into()).unwrap();
        store.set("t3", "k2".into(), "v2".into()).unwrap();

        let iter = store.get_iter("t3").unwrap();
        let mut pairs = iter.collect::<Vec<_>>();
        pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            pairs,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        );
    }

    use tempfile::tempdir;

    #[test]
    fn sleddb_basic_interface_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_basic_interface(store);
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
}
