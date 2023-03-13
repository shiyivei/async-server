use dashmap::{DashMap, DashSet};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::sync::mpsc;

use crate::error::KvError;
use crate::{CommandResponse, Value};
use tracing::{debug, info, warn};

//topic里最大存放的数据
const BROADCAST_CAPACITY: usize = 128;

//下一个
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

// 获取下一个id
fn get_next_subscription_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

// 主题 trait 有方法
pub trait Topic: Send + Sync + 'static {
    //订阅
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>>;
    //取消
    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError>;
    //往主题中发布数据
    fn publish(self, name: String, value: Arc<CommandResponse>);
}

// 主题发布和订阅
#[derive(Default)]
pub struct Broadcaster {
    topics: DashMap<String, DashSet<u32>>,
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

        // 生成一个mpsc channel
        let (tx, rx) = mpsc::channel(BROADCAST_CAPACITY);

        let v: Value = (id as i64).into();

        let tx1 = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = tx1.send(Arc::new(v.into())).await {
                warn!("Failed to send subscription id: {},Error: {:?}", id, e)
            }
        });

        self.subscriptions.insert(id, tx);
        debug!("Subscription {} is added", id);

        rx
    }

    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError> {
        if let Some(v) = self.topics.get_mut(&name) {
            v.remove(&id);

            if v.is_empty() {
                info!("Topic: {:?} is deleted", &name);
                drop(v);
                self.topics.remove(&name);
            }
        }

        debug!("Subscription {} is removed!", id);
        self.subscriptions.remove(&id);

        Ok(id)
    }

    fn publish(self, name: String, value: Arc<CommandResponse>) {
        tokio::spawn(async move {
            match self.topics.get(&name) {
                Some(chan) => {
                    let chan = chan.value().clone();

                    for id in chan.into_iter() {
                        if let Some(tx) = self.subscriptions.get(&id) {
                            if let Err(e) = tx.send(value.clone()).await {
                                warn!("Publish to {} failed! error: {:?}", id, e);
                            }
                        }
                    }
                }
                None => {}
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use tokio::sync::mpsc::Receiver;

    use crate::assert_res_ok;

    use super::*;

    #[tokio::test]
    async fn pub_sub_should_work() {
        let b = Arc::new(Broadcaster::default());
        let lobby = "lobby".to_string();

        // subscribe
        let mut stream1 = b.clone().subscribe(lobby.clone());
        let mut stream2 = b.clone().subscribe(lobby.clone());

        // publish
        let v: Value = "hello".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));

        // subscribers 应该能收到 publish 的数据
        let id1 = get_id(&mut stream1).await;
        let id2 = get_id(&mut stream2).await;
        println!("Got id {:?}", id1);
        println!("Got id {:?}", id2);

        assert!(id1 != id2);

        let res1 = stream1.recv().await.unwrap();
        let res2 = stream2.recv().await.unwrap();

        assert_eq!(res1, res2);
        assert_res_ok(&res1, &[v.clone()], &[]);

        // 如果 subscriber 取消订阅，则收不到新数据
        let result = b.clone().unsubscribe(lobby.clone(), id1 as _).unwrap();
        assert_eq!(result, id1 as _);

        // publish
        let v: Value = "world".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));

        assert!(stream1.recv().await.is_none());
        let res2 = stream2.recv().await.unwrap();
        assert_res_ok(&res2, &[v.clone()], &[]);
    }

    pub async fn get_id(res: &mut Receiver<Arc<CommandResponse>>) -> u32 {
        let id: i64 = res.recv().await.unwrap().as_ref().try_into().unwrap();
        println!("get_id {:?}", id);
        id as u32
    }
}
