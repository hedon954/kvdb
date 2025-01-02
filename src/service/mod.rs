mod command_service;
mod topic;

use std::sync::Arc;

use tracing::debug;

use crate::{CommandRequest, CommandResponse, KvError, MemTable, RequestData, Storage};

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
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>,
    on_before_send: Vec<fn(&mut CommandResponse)>,
    on_after_send: Vec<fn()>,
}

impl<Store: Storage> Service<Store> {
    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Got request: {:?}", cmd);
        self.inner.on_received.notify(&cmd);
        let mut res = dispatch(cmd, &self.inner.store);
        debug!("Executed response: {:?}", &res);
        self.inner.on_executed.notify(&res);
        self.inner.on_before_send.notify(&mut res);
        if !self.inner.on_after_send.is_empty() {
            debug!("Modified response: {:?}", &res);
        }
        res
    }
}

impl<Store: Storage> ServiceInner<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            on_received: Vec::new(),
            on_executed: Vec::new(),
            on_before_send: Vec::new(),
            on_after_send: Vec::new(),
        }
    }

    pub fn fn_received(mut self, f: fn(&CommandRequest)) -> Self {
        self.on_received.push(f);
        self
    }

    pub fn fn_executed(mut self, f: fn(&CommandResponse)) -> Self {
        self.on_executed.push(f);
        self
    }

    pub fn fn_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(f);
        self
    }

    pub fn fn_after_send(mut self, f: fn()) -> Self {
        self.on_after_send.push(f);
        self
    }
}

impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

/// A trait for notify, without mut
pub trait Notify<Arg> {
    fn notify(&self, arg: &Arg);
}

/// A trait for notify, with mut
pub trait NotifyMut<Arg> {
    fn notify(&self, arg: &mut Arg);
}

impl<Arg> Notify<Arg> for Vec<fn(&Arg)> {
    fn notify(&self, arg: &Arg) {
        for f in self {
            f(arg);
        }
    }
}

impl<Arg> NotifyMut<Arg> for Vec<fn(&mut Arg)> {
    fn notify(&self, arg: &mut Arg) {
        for f in self {
            f(arg);
        }
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
pub fn assert_res_ok(res: &CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    let mut sorted_pairs = res.pairs.to_vec();
    sorted_pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(sorted_pairs, pairs);
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
    use http::StatusCode;
    use tracing::info;

    use super::*;
    use crate::{CommandRequest, MemTable, Value};
    use std::thread;

    #[test]
    fn service_should_work() {
        let service: Service = ServiceInner::new(MemTable::new()).into();

        let cloned = service.clone();

        // create a new thread to set k1, v1, it should return none
        let handle = thread::spawn(move || {
            let res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(&res, &[Value::default()], &[]);
        });
        handle.join().unwrap();

        // get k1 on current thread, it should return v1
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(&res, &["v1".into()], &[]);
    }

    #[test]
    fn event_registration_should_work() {
        fn b(cmd: &CommandRequest) {
            info!("Received command: {:?}", cmd);
        }
        fn c(res: &CommandResponse) {
            info!("Executed command: {:?}", res);
        }
        fn d(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as _;
            info!("Before send command: {:?}", res);
        }
        fn e() {
            info!("After send command");
        }
        let service: Service = ServiceInner::new(MemTable::new())
            .fn_received(|_: &CommandRequest| {})
            .fn_received(b)
            .fn_executed(c)
            .fn_before_send(d)
            .fn_after_send(e)
            .into();

        let res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        assert_eq!(res.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(res.message, "");
        assert_eq!(res.values, vec![Value::default()]);
    }
}
