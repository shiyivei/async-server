//! 服务模块，将外部网络请求转换为内部数据库指令
//!

mod command_service;
mod top;
mod topic_service;
use crate::error::KvError;
use crate::pb::command_request::RequestData;
use crate::storage::SledDb;
use crate::storage::Storage;
use futures::stream;
use http::StatusCode;

use std::sync::Arc;
use tracing::debug;
use tracing::info;

use crate::{pb::*, MemTable};
pub use command_service::*;

// 让数据对象能够多线程访问
pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
    broadcaster: Arc<Broadcaster>,
}

// 手动实现clone
impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            broadcaster: Arc::clone(&self.broadcaster),
        }
    }
}

// impl<Store: Storage> Service<Store> {
//     // pub fn new(store: Store) -> Self {
//     //     Self {
//     //         inner: Arc::new(ServiceInner { store }),
//     //     }
//     // }
//     pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
//         debug!("Got request: {:?}", cmd);
//         // 接收后发送消息
//         self.inner.on_received.notify(&cmd);
//         let mut res = dispatch(cmd, &self.inner.store);
//         debug!("Executed response: {:?}", res);
//         // 处理完请求发送消息
//         self.inner.on_executed.notify(&res);
//         // 发送处理结果之前再通知一遍
//         self.inner.on_before_send.notify(&mut res);

//         if !self.inner.on_before_send.is_empty() {
//             debug!("Modified response: {:?}", res);
//         }
//         res
//     }
// }

pub struct ServiceInner<Store> {
    store: Store,
    on_received: Vec<fn(&CommandRequest)>, // 函数指针，服务器收到请求时触发
    on_executed: Vec<fn(&CommandResponse)>, // 处理完请求时触发
    on_before_send: Vec<fn(&mut CommandResponse)>, // 在发送之前修改
    on_after_send: Vec<fn()>,              // 发送完响应后触发
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

pub trait Notify<Arg> {
    fn notify(&self, arg: &Arg);
}

pub trait NotifyMut<Arg> {
    fn notify(&self, arg: &mut Arg);
}

impl<Arg> Notify<Arg> for Vec<fn(&Arg)> {
    #[inline]
    fn notify(&self, arg: &Arg) {
        for f in self {
            f(arg)
        }
    }
}

impl<Arg> NotifyMut<Arg> for Vec<fn(&mut Arg)> {
    #[inline]
    fn notify(&self, arg: &mut Arg) {
        for f in self {
            f(arg)
        }
    }
}

// 从 Request 中得到 Response，目前处理 HGET/HGETALL/HSET
// 拿到了命令和数据库
pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hgetall(param)) => param.execute(store),
        Some(RequestData::Hmget(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        Some(RequestData::Hmset(param)) => param.execute(store),
        Some(RequestData::Hdel(param)) => param.execute(store),
        Some(RequestData::Hmdel(param)) => param.execute(store),
        Some(RequestData::Hexist(param)) => param.execute(store),
        Some(RequestData::Hmexist(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(), // 处理不了的返回一个啥都不包括的 Response，这样后续可以用 dispatch_stream 处理
        _ => CommandResponse::default(),
    }
}

// 重新做一个
pub fn dispatch_stream(cmd: CommandRequest, topic: impl Topic) -> StreamingResponse {
    match cmd.request_data {
        Some(RequestData::Publish(param)) => param.execute(topic),
        Some(RequestData::Subscribe(param)) => param.execute(topic),
        Some(RequestData::Unsubscribe(param)) => param.execute(topic),

        _ => unreachable!(),
    }
}

impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
            broadcaster: Default::default(),
        }
    }
}

impl<Store: Storage> Service<Store> {
    pub fn execute(&self, cmd: CommandRequest) -> StreamingResponse {
        debug!("Got a result: {:?}", cmd);

        self.inner.on_received.notify(&cmd);
        let mut res = dispatch(cmd.clone(), &self.inner.store);

        if res == CommandResponse::default() {
            dispatch_stream(cmd, Arc::clone(&self.broadcaster))
        } else {
            debug!("Executed response: {:?}", res);
            self.inner.on_executed.notify(&res);
            self.inner.on_before_send.notify(&mut res);
            if !self.inner.on_before_send.is_empty() {
                debug!("Modified response: {:?}", res);
            }
            Box::pin(stream::once(async { Arc::new(res) }))
        }
    }
}
#[cfg(test)]

mod tests {
    use std::thread;

    use futures::StreamExt;

    use super::*;
    use crate::{storage::MemTable, Value};

    #[tokio::test]
    async fn service_should_works() {
        // 我们需要一个 service 结构至少包含 Storage
        let service: Service = ServiceInner::new(MemTable::default()).into();

        // service 可以运行在多线程环境下，它的 clone 应该是轻量级的
        let cloned = service.clone();

        // 创建一个线程，在 table t1 中写入 k1, v1
        tokio::spawn(async move {
            let mut res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            let data = res.next().await.unwrap();
            assert_res_ok(&data, &[Value::default()], &[]);
        })
        .await
        .unwrap();

        // 在当前线程下读取 table t1 的 k1，应该返回 v1
        let mut res = service.execute(CommandRequest::new_hget("t1", "k1"));
        let data = res.next().await.unwrap();
        assert_res_ok(&data, &["v1".into()], &[]);
    }

    #[tokio::test]
    async fn event_registration_should_work() {
        fn b(cmd: &CommandRequest) {
            info!("Got {:?}", cmd);
        }
        fn c(res: &CommandResponse) {
            info!("{:?}", res);
        }
        fn d(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as _;
        }
        fn e() {
            info!("Data is sent");
        }

        let service: Service = ServiceInner::new(MemTable::default())
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

#[cfg(test)]
use crate::{Kvpair, Value};

use self::top::Broadcaster;
use self::top::Topic;
use self::topic_service::StreamingResponse;
use self::topic_service::TopicService;

// 测试成功返回的结果
#[cfg(test)]
pub async fn assert_res_ok(res: &CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    let mut sorted_pairs = res.pairs.clone();
    sorted_pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

// 测试失败返回的结果
#[cfg(test)]
pub async fn assert_res_error(res: &CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}
