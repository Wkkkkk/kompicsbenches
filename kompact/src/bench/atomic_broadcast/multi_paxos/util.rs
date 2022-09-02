use std::cmp::{max};
use kompact::prelude::Port;
use crate::bench::atomic_broadcast::multi_paxos::messages::{BatcherInbound, CommandBatchOrNoop, LeaderInbound, ProxyLeaderInbound};

pub type Round = u64;
pub type Slot = u64;

/// https://github.com/mwhittaker/frankenpaxos/blob/master/shared/src/main/scala/frankenpaxos/roundsystem/RoundSystem.scala#L60
pub struct ClassicRoundRobin {
    pub(crate) n: u64,
}

impl ClassicRoundRobin {
    pub(crate) fn leader(&self, round: Round) -> u64 {
        round % self.n
    }

    pub(crate) fn next_classic_round(&self, leader_index: u64, round: Round) -> Round {
        if round < 1 {  //  1 indexed since we use u64
            leader_index
        } else {
            let smallest_multiple_of_n = self.n * (round / self.n);
            let offset = leader_index % self.n;
            if (smallest_multiple_of_n + offset) > round {
                smallest_multiple_of_n + offset
            } else {
                smallest_multiple_of_n + self.n + offset
            }
        }
    }
}

/// https://github.com/mwhittaker/frankenpaxos/blob/master/shared/src/main/scala/frankenpaxos/util/BufferMap.scala
pub struct BufferMap {
    grow_size: usize,
    buffer: Vec<Option<CommandBatchOrNoop>>,
    watermark: u64,
    largest_key: Option<u64>,
}

impl BufferMap {
    pub fn with(grow_size: usize) -> Self {
        Self {
            grow_size,
            buffer: Vec::with_capacity(grow_size),
            watermark: 0,
            largest_key: None
        }
    }

    pub fn normalize(&self, key: u64) -> Option<u64> {
        if key >= self.watermark { Some(key-self.watermark) } else { None }
    }

    pub fn pad(&mut self, len: usize) {
        while self.buffer.len() < len {
            self.buffer.push(None);
        }
    }

    pub fn get(&self, key: u64) -> Option<CommandBatchOrNoop> {
        let normalized = self.normalize(key);
        match normalized {
            Some(normalized) if normalized >= self.buffer.len() as u64 => None,
            None => None,
            Some(normalized) => self.buffer.get(normalized as usize).expect("Failed to get").clone()
        }
    }

    pub fn put(&mut self, key: u64, value: CommandBatchOrNoop) {
        self.largest_key = Some(max(self.largest_key.unwrap_or_default(), key));
        match self.normalize(key) {
            Some(normalized) => {
                if normalized < self.buffer.len() as u64 {
                    self.buffer[normalized as usize] = Some(value);
                } else {
                    self.pad(normalized as usize + 1 + self.grow_size);
                    self.buffer[normalized as usize] = Some(value);
                }
            }
            None => {}
        }
    }

    pub fn contains(&self, key: u64) -> bool {
        self.get(key).is_some()
    }

    fn garbage_collect(&mut self, _watermark: u64) {
        unimplemented!("Should not run GC");
        /*
        if watermark <= self.watermark {
            return;
        }

        let (_removed, rem) = self.buffer.split_at(min((watermark - self.watermark) as usize, self.buffer.len()));
        self.buffer = rem;
        self.watermark = watermark;
        */
    }
}

pub struct LeaderElectionPort;

impl Port for LeaderElectionPort {
    type Indication = u64;  // elected leader pid
    type Request = ();
}

pub struct ProxyLeaderPort;

impl Port for ProxyLeaderPort {
    type Indication = ();
    type Request = ProxyLeaderInbound;
}

pub struct BatcherPort;

impl Port for BatcherPort {
    type Indication = LeaderInbound;
    type Request = BatcherInbound;
}