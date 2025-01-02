#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct CommandRequest {
    #[prost(
        oneof = "command_request::RequestData",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12"
    )]
    pub request_data: ::core::option::Option<command_request::RequestData>,
}
/// Nested message and enum types in `CommandRequest`.
pub mod command_request {
    #[derive(PartialOrd, Clone, PartialEq, ::prost::Oneof)]
    pub enum RequestData {
        #[prost(message, tag = "1")]
        Hget(super::Hget),
        #[prost(message, tag = "2")]
        Hgetall(super::Hgetall),
        #[prost(message, tag = "3")]
        Hmget(super::Hmget),
        #[prost(message, tag = "4")]
        Hset(super::Hset),
        #[prost(message, tag = "5")]
        Hmset(super::Hmset),
        #[prost(message, tag = "6")]
        Hdel(super::Hdel),
        #[prost(message, tag = "7")]
        Hmdel(super::Hmdel),
        #[prost(message, tag = "8")]
        Hexist(super::Hexist),
        #[prost(message, tag = "9")]
        Hmexist(super::Hmexist),
        #[prost(message, tag = "10")]
        Subscribe(super::Subscribe),
        #[prost(message, tag = "11")]
        Unsubscribe(super::Unsubscribe),
        #[prost(message, tag = "12")]
        Publish(super::Publish),
    }
}
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct CommandResponse {
    /// use the same status code as HTTP, like 2xx/4xx/5xx
    #[prost(uint32, tag = "1")]
    pub status: u32,
    /// error message if the status code is not 2xx
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
    /// if success, return the values of the key-value pairs
    #[prost(message, repeated, tag = "3")]
    pub values: ::prost::alloc::vec::Vec<Value>,
    /// if success, return the key-value pairs
    #[prost(message, repeated, tag = "4")]
    pub pairs: ::prost::alloc::vec::Vec<Kvpair>,
}
/// get a key-value pair from the given table
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hget {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub key: ::prost::alloc::string::String,
}
/// get all key-value pairs of the given table
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hgetall {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
}
/// get multiple keys from the given table
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hmget {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// set a key-value pair
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hset {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub pair: ::core::option::Option<Kvpair>,
}
/// set multiple key-value pairs
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hmset {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub pairs: ::prost::alloc::vec::Vec<Kvpair>,
}
/// delete a key, and return the value of the deleted key
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hdel {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub key: ::prost::alloc::string::String,
}
/// delete multiple keys, and return the values of the deleted keys
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hmdel {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// check if the key exists in the given table
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hexist {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub key: ::prost::alloc::string::String,
}
/// check if multiple keys exist in the given table
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Hmexist {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Value {
    #[prost(oneof = "value::Value", tags = "1, 2, 3, 4, 5")]
    pub value: ::core::option::Option<value::Value>,
}
/// Nested message and enum types in `Value`.
pub mod value {
    #[derive(PartialOrd, Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        String(::prost::alloc::string::String),
        #[prost(bytes, tag = "2")]
        Binary(::prost::bytes::Bytes),
        #[prost(int64, tag = "3")]
        Integer(i64),
        #[prost(double, tag = "4")]
        Float(f64),
        #[prost(bool, tag = "5")]
        Bool(bool),
    }
}
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Kvpair {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<Value>,
}
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Subscribe {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
}
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Unsubscribe {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub id: u32,
}
#[derive(PartialOrd, Clone, PartialEq, ::prost::Message)]
pub struct Publish {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub values: ::prost::alloc::vec::Vec<Value>,
}
