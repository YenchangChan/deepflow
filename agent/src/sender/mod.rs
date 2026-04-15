/*
 * Copyright (c) 2024 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::atomic::{AtomicU8, Ordering};
use std::thread::JoinHandle;

use public::sender::Sendable;

use self::lumberjack_sender::LumberjackSenderThread;
use self::uniform_sender::UniformSenderThread;

// NpbBandwidthWatcher NewFragmenterBuilder NewCompressorBuilder NewPCapBuilder NewUniformCollectSender
pub(crate) mod lumberjack_sender;
pub mod npb_sender;
mod tcp_packet;
pub(crate) mod uniform_sender;

static ID_COUNTER: AtomicU8 = AtomicU8::new(0);

// get unique sender_id avoid handwrite sender_id
pub fn get_sender_id() -> u8 {
    ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

pub(crate) const QUEUE_BATCH_SIZE: usize = 1024;

/// Sender that can be either UniformSender (TCP/Protobuf) or LumberjackSender,
/// depending on outputs.lumberjack.enabled configuration.
pub enum SendHandler<T: Sendable> {
    Uniform(UniformSenderThread<T>),
    Lumberjack(LumberjackSenderThread<T>),
}

impl<T: Sendable> SendHandler<T> {
    pub fn start(&mut self) {
        match self {
            Self::Uniform(s) => s.start(),
            Self::Lumberjack(s) => s.start(),
        }
    }

    pub fn notify_stop(&mut self) -> Option<JoinHandle<()>> {
        match self {
            Self::Uniform(s) => s.notify_stop(),
            Self::Lumberjack(s) => s.notify_stop(),
        }
    }

    pub fn stop(&mut self) {
        match self {
            Self::Uniform(s) => s.stop(),
            Self::Lumberjack(s) => s.stop(),
        }
    }
}
