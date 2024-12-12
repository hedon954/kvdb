use dashmap::{mapref::one::Ref, DashMap};

use crate::{Kvpair, Value};

use super::Storage;

/// A simple in-memory key-value storage engine built on top of dashmap.
/// It is thread-safe and supports concurrent read and write operations.
#[derive(Debug, Default, Clone)]
pub struct MemTable {
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    /// Create a default MemTable
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a table if it does not exist, and return a reference to it.
    pub fn get_or_create_table(&self, name: &str) -> Ref<String, DashMap<String, Value>> {
        match self.tables.get(name) {
            Some(table) => table,
            None => {
                let entry = self.tables.entry(name.into()).or_default();
                entry.downgrade()
            }
        }
    }
}

impl Storage for MemTable {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.get(key).map(|v| v.value().clone()))
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.insert(key, value))
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.contains_key(key))
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.remove(key).map(|(_k, v)| v))
    }

    fn get_all(&self, table: &str) -> Result<Vec<crate::Kvpair>, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table
            .iter()
            .map(|kv| Kvpair::new(kv.key(), kv.value().clone()))
            .collect())
    }

    #[allow(unused)]
    fn get_iter(
        &self,
        table: &str,
    ) -> Result<Box<dyn Iterator<Item = crate::Kvpair>>, crate::KvError> {
        todo!()
    }
}
