// Copyright (c) 2015 Y. T. Chung <zonyitoo@gmail.com>
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>,
// at your option. All files in the project carrying such
// notice may not be copied, modified, or distributed except
// according to those terms.

use std::collections::{BTreeMap, HashMap};
use std::error;
use std::fmt;
use std::io::{BufRead, BufReader, Cursor, Write};
use std::str;
use std::string::String;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use log::debug;
use semver::Version;

use crate::proto::{self, AuthResponse, MemCachedResult};
use proto::textdef::{Command, DataType, RequestHeader, RequestPacket, RequestPacketRef, ResponsePacket};
use proto::{AuthOperation, CasOperation, MultiOperation, NoReplyOperation, Operation, ServerOperation};

pub use proto::textdef::Status;

#[derive(Debug, Clone)]
pub struct Error {
    status: Status,
    desc: &'static str,
    detail: Option<String>,
}

impl Error {
    fn from_status(status: Status, detail: Option<String>) -> Error {
        Error {
            status,
            desc: status.desc(),
            detail,
        }
    }

    /// Get error description
    pub fn detail(&self) -> Option<String> {
        self.detail.clone()
    }

    /// Get status code
    pub fn status(&self) -> Status {
        self.status
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.desc)?;
        match self.detail {
            Some(ref s) => write!(f, " ({})", s),
            None => Ok(()),
        }
    }
}

impl error::Error for Error {}

pub struct TextProto<T: BufRead + Write + Send> {
    stream: T,
}

impl<T: BufRead + Write + Send> TextProto<T> {
    pub fn new(stream: T) -> TextProto<T> {
        TextProto { stream }
    }

    fn send_noop(&mut self) -> MemCachedResult<u32> {
        panic!("NoOp command no supported for text protocol.")
    }
}

impl<T: BufRead + Write + Send> Operation for TextProto<T> {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        panic!("Set command no supported for text protocol.")
    }

    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        panic!("Add command no supported for text protocol.")
    }

    fn delete(&mut self, key: &[u8]) -> MemCachedResult<()> {
        panic!("Delete command no supported for text protocol.")
    }

    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        panic!("Replace command no supported for text protocol.")
    }

    fn get(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32)> {
        panic!("get command no supported for text protocol.")
    }

    fn getk(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32)> {
        panic!("getk command no supported for text protocol.")
    }

    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<u64> {
        panic!("increment command no supported for text protocol.")
    }

    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<u64> {
        panic!("decrement command no supported for text protocol.")
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        panic!("append command no supported for text protocol.")
    }

    fn prepend(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        panic!("prepend command no supported for text protocol.")
    }

    fn touch(&mut self, key: &[u8], expiration: u32) -> MemCachedResult<()> {
        panic!("touch command no supported for text protocol.")
    }
}

impl<T: BufRead + Write + Send> ServerOperation for TextProto<T> {
    fn quit(&mut self) -> MemCachedResult<()> {
        panic!("quit command no supported for text protocol.")
    }

    fn flush(&mut self, expiration: u32) -> MemCachedResult<()> {
        panic!("flush command no supported for text protocol.")
    }

    fn noop(&mut self) -> MemCachedResult<()> {
        panic!("noop command no supported for text protocol.")
    }

    fn version(&mut self) -> MemCachedResult<Version> {
        panic!("version command no supported for text protocol.")
    }

    fn stat(&mut self) -> MemCachedResult<BTreeMap<String, String>> {
        panic!("stat command no supported for text protocol.")
    }
}

impl<T: BufRead + Write + Send> MultiOperation for TextProto<T> {
    fn set_multi(&mut self, kv: BTreeMap<&[u8], (&[u8], u32, u32)>) -> MemCachedResult<()> {
        panic!("set_multi command no supported for text protocol.")
    }

    fn delete_multi(&mut self, keys: &[&[u8]]) -> MemCachedResult<()> {
        panic!("delete_multi command no supported for text protocol.")
    }

    fn increment_multi<'a>(
        &mut self,
        kv: HashMap<&'a [u8], (u64, u64, u32)>,
    ) -> MemCachedResult<HashMap<&'a [u8], u64>> {
        panic!("increment_multi command no supported for text protocol.")
    }

    fn get_multi(&mut self, keys: &[&[u8]]) -> MemCachedResult<HashMap<Vec<u8>, (Vec<u8>, u32)>> {
        panic!("get_multi command no supported for text protocol.")
    }
}

impl<T: BufRead + Write + Send> NoReplyOperation for TextProto<T> {
    fn set_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        panic!("set_noreply command no supported for text protocol.")
    }

    fn add_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        panic!("add_noreply command no supported for text protocol.")
    }

    fn delete_noreply(&mut self, key: &[u8]) -> MemCachedResult<()> {
        panic!("delete_noreply command no supported for text protocol.")
    }

    fn replace_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<()> {
        panic!("replace_noreply command no supported for text protocol.")
    }

    fn increment_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()> {
        panic!("increment_noreply command no supported for text protocol.")
    }

    fn decrement_noreply(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> MemCachedResult<()> {
        panic!("decrement_noreply command no supported for text protocol.")
    }

    fn append_noreply(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        panic!("append_noreply command no supported for text protocol.")
    }

    fn prepend_noreply(&mut self, key: &[u8], value: &[u8]) -> MemCachedResult<()> {
        panic!("prepend_noreply command no supported for text protocol.")
    }
}

impl<T: BufRead + Write + Send> CasOperation for TextProto<T> {
    fn set_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> MemCachedResult<u64> {
        panic!("set_cas command no supported for text protocol.")
    }

    fn add_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> MemCachedResult<u64> {
        panic!("add_cas command no supported for text protocol.")
    }

    fn replace_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32, cas: u64) -> MemCachedResult<u64> {
        panic!("replace_cas command no supported for text protocol.")
    }

    fn get_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, u32, u64)> {
        panic!("get_cas command no supported for text protocol.")
    }

    fn getk_cas(&mut self, key: &[u8]) -> MemCachedResult<(Vec<u8>, Vec<u8>, u32, u64)> {
        panic!("getk_cas command no supported for text protocol.")
    }

    fn increment_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> MemCachedResult<(u64, u64)> {
        panic!("increment_cas command no supported for text protocol.")
    }

    fn decrement_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> MemCachedResult<(u64, u64)> {
        panic!("decrement_cas command no supported for text protocol.")
    }

    fn append_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64> {
        panic!("append_cas command no supported for text protocol.")
    }

    fn prepend_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> MemCachedResult<u64> {
        panic!("prepend_cas command no supported for text protocol.")
    }

    fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> MemCachedResult<u64> {
        panic!("touch_cas command no supported for text protocol.")
    }
}

impl<T: BufRead + Write + Send> AuthOperation for TextProto<T> {
    fn list_mechanisms(&mut self) -> MemCachedResult<Vec<String>> {
        panic!("list_mechanisms command no supported for text protocol.")
    }

    fn auth_start(&mut self, mech: &str, init: &[u8]) -> MemCachedResult<AuthResponse> {
        panic!("auth_start command no supported for text protocol.")
    }

    fn auth_continue(&mut self, mech: &str, data: &[u8]) -> MemCachedResult<AuthResponse> {
        panic!("auth_continue command no supported for text protocol.")
    }
}