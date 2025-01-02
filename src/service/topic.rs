use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{CommandResponse, Value};

/// The capacity of a topic.
const BROADCAST_CAPACITY: usize = 128;

/// The next id generator of a subscription.
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

/// Get the next id of a subscription.
fn get_next_subscription_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// A trait for a topic.
#[allow(unused)]
pub trait Topic: Send + Sync + 'static {
    /// Subscribe to a topic.
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>>;
    /// Unsubscribe from a topic.
    fn unsubscribe(self, name: String, id: u32);
    /// Publish a message to a topic.
    fn publish(self, name: String, value: Arc<CommandResponse>);
}

/// A broadcaster for topics.
#[derive(Default)]
pub struct Broadcaster {
    /// The topics, key is the topic name, value is the set of subscription ids.
    topics: DashMap<String, DashSet<u32>>,
    /// The subscriptions, key is the subscription id, value is the sender of the subscription.
    subscriptions: DashMap<u32, mpsc::Sender<Arc<CommandResponse>>>,
}

impl Topic for Arc<Broadcaster> {
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>> {
        let id = {
            let entry = self.topics.entry(name).or_default();
            let id = get_next_subscription_id();
            entry.value().insert(id);
            id
        };

        let (tx, rx) = mpsc::channel(BROADCAST_CAPACITY);

        let v: Value = (id as i64).into();

        let tx1 = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = tx1.send(Arc::new(v.into())).await {
                warn!("Failed to send subscription id: {}. Error: {:?}", id, e);
            }
        });

        self.subscriptions.insert(id, tx);
        debug!("Subscription {} is added", id);

        rx
    }

    fn unsubscribe(self, name: String, id: u32) {
        _ = self.remove_subscription(name, id);
    }

    fn publish(self, name: String, value: Arc<CommandResponse>) {
        tokio::spawn(async move {
            let mut ids = vec![];
            if let Some(topic) = self.topics.get(&name) {
                let subscriptions = topic.value().clone();

                drop(topic); // unlock quickly

                for id in subscriptions.into_iter() {
                    if let Some(tx) = self.subscriptions.get(&id) {
                        if let Err(e) = tx.send(value.clone()).await {
                            warn!("Failed to send message to subscription {}, {}", id, e);
                            ids.push(id);
                        }
                    }
                }
            }

            for id in ids {
                _ = self.remove_subscription(name.clone(), id);
            }
        });
    }
}

impl Broadcaster {
    pub fn remove_subscription(&self, name: String, id: u32) -> Option<u32> {
        if let Some(v) = self.topics.get_mut(&name) {
            v.remove(&id);
            if v.is_empty() {
                info!("Topic is empty, removing it: {}", name);
                drop(v); // unlock quickly
                self.topics.remove(&name);
            }
        }
        debug!("Unsubscribed from topic: {}, id: {}", name, id);
        self.subscriptions.remove(&id).map(|(id, _)| id)
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_res_ok;

    use super::*;
    use std::convert::TryInto;

    #[tokio::test]
    async fn pub_sub_should_work() {
        let b = Arc::new(Broadcaster::default());
        let lobby = "lobby".to_string();

        // subscribe to the lobby topic.
        let mut stream1 = b.clone().subscribe(lobby.clone());
        let mut stream2 = b.clone().subscribe(lobby.clone());

        // publish a message to the lobby topic.
        let v: Value = "hello".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));

        // subscribers should be able to receive the message.
        let id1: i64 = stream1.recv().await.unwrap().as_ref().try_into().unwrap();
        let id2: i64 = stream2.recv().await.unwrap().as_ref().try_into().unwrap();

        assert!(id1 != id2); // different ids

        // the message should be the same
        let res1 = stream1.recv().await.unwrap();
        let res2 = stream2.recv().await.unwrap();
        assert_eq!(res1, res2);
        assert_res_ok(&res1, &[v.clone()], &[]);

        // if unsubscribe, the subscriber should not receive the message.
        b.clone().unsubscribe(lobby.clone(), id1 as u32);

        // publish a message to the lobby topic.
        let v: Value = "world".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));

        // the subscriber should not receive the message.
        assert!(stream1.recv().await.is_none());

        // the other subscriber should receive the message.
        let res2 = stream2.recv().await.unwrap();
        assert_res_ok(&res2, &[v.clone()], &[]);
    }
}
