mod command_service;
mod topic;
mod topic_service;

use std::sync::Arc;

use futures::stream;
use topic::{Broadcaster, Topic};
use topic_service::{StreamingResponse, TopicService};
use tracing::{debug, info};

use crate::{CommandRequest, CommandResponse, KvError, MemTable, RequestData, Storage};

/// A trait for command service
pub trait CommandService {
    /// Execute the command and return the `CommandResponse`
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
    broadcaster: Arc<Broadcaster>,
}

pub struct ServiceInner<Store> {
    store: Store,
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>,
    on_before_send: Vec<fn(&mut CommandResponse)>,
    on_after_send: Vec<fn()>,
}

impl<Store: Storage> Service<Store> {
    pub fn execute(&self, cmd: CommandRequest) -> StreamingResponse {
        debug!("Got request: {:?}", cmd);
        self.inner.on_received.notify(&cmd);
        let mut res = dispatch(cmd.clone(), &self.inner.store);

        if res == CommandResponse::default() {
            dispatch_stream(cmd, Arc::clone(&self.broadcaster))
        } else {
            debug!("Executed response: {:?}", &res);
            self.inner.on_executed.notify(&res);
            self.inner.on_before_send.notify(&mut res);
            if !self.inner.on_after_send.is_empty() {
                debug!("Modified response: {:?}", &res);
            }

            Box::pin(stream::once(async { Arc::new(res) }))
        }
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

impl<Store: Storage> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            broadcaster: Arc::clone(&self.broadcaster),
        }
    }
}

impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
            broadcaster: Arc::new(Broadcaster::default()),
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
        // return a default response, and use dispatch_stream to handle the stream
        _ => CommandResponse::default(),
    }
}

pub fn dispatch_stream(cmd: CommandRequest, topic: impl Topic) -> StreamingResponse {
    info!("Dispatching stream: {:?}", cmd);
    match cmd.request_data {
        Some(RequestData::Subscribe(req)) => req.execute(topic),
        Some(RequestData::Unsubscribe(req)) => req.execute(topic),
        Some(RequestData::Publish(req)) => req.execute(topic),
        _ => unreachable!(),
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
pub fn assert_res_error(res: &CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, vec![]);
    assert_eq!(res.pairs, vec![]);
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use stream::StreamExt;
    use tracing::info;

    use super::*;
    use crate::{CommandRequest, MemTable, Value};

    #[tokio::test]
    async fn service_should_work() {
        let service: Service = ServiceInner::new(MemTable::new()).into();

        let cloned = service.clone();

        // create a new thread to set k1, v1, it should return none
        let handle = tokio::spawn(async move {
            let mut res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            let data = res.next().await.unwrap();
            assert_res_ok(&data, &[Value::default()], &[]);
        });
        handle.await.unwrap();

        // get k1 on current thread, it should return v1
        let mut res = service.execute(CommandRequest::new_hget("t1", "k1"));
        let data = res.next().await.unwrap();
        assert_res_ok(&data, &["v1".into()], &[]);
    }

    #[tokio::test]
    async fn event_registration_should_work() {
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

        let mut res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        let data = res.next().await.unwrap();
        assert_eq!(data.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(data.message, "");
        assert_eq!(data.values, vec![Value::default()]);
    }
}
