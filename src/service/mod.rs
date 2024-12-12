use std::sync::Arc;

use tracing::debug;

use crate::{CommandRequest, CommandResponse, KvError, MemTable, RequestData, Storage};

mod command_service;

/// A trait for command service
pub trait CommandService {
    /// Execute the command and return the `CommandResponse`
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

#[derive(Clone)]
pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
}

pub struct ServiceInner<Store> {
    store: Store,
}

impl<Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner { store }),
        }
    }

    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Got request: {:?}", cmd);
        let res = dispatch(cmd, &self.inner.store);
        debug!("Executed response: {:?}", res);

        // TODO: send `on_executed` event
        res
    }
}

pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(req)) => req.execute(store),
        Some(RequestData::Hset(req)) => req.execute(store),
        Some(RequestData::Hgetall(req)) => req.execute(store),
        None => KvError::InvalidCommand("Request has not data".into()).into(),
        _ => KvError::Internal("Not implemented".into()).into(),
    }
}

#[cfg(test)]
use crate::{Kvpair, Value};

#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, vec![]);
    assert_eq!(res.pairs, vec![]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandRequest, MemTable, Value};
    use std::thread;

    #[test]
    fn service_should_work() {
        let service = Service::new(MemTable::new());

        let cloned = service.clone();

        // create a new thread to set k1, v1, it should return none
        let handle = thread::spawn(move || {
            let res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[]);
        });
        handle.join().unwrap();

        // get k1 on current thread, it should return v1
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }
}
