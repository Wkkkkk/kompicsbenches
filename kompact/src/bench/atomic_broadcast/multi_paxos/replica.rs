use std::time::Duration;
use hashbrown::HashMap;
use kompact::prelude::*;
use crate::bench::atomic_broadcast::messages::{AtomicBroadcastMsg, ProposalResp};
use crate::bench::atomic_broadcast::multi_paxos::messages::{Chosen, ChosenWatermark, CommandBatchOrNoop, LeaderInbound};
use crate::bench::atomic_broadcast::multi_paxos::util::{BufferMap, LeaderElectionPort, Slot};
use super::serializers::ReplicaInboundSer;
use super::messages::ReplicaInbound;

pub struct ReplicaOptions {
    pub(crate) log_grow_size: usize,
    pub(crate) send_chosen_watermark_every_n_entries: u64,
    pub(crate) recover_log_entry_min_period: Duration,
    pub(crate) recover_log_entry_max_period: Duration,
}
#[derive(ComponentDefinition)]
pub struct Replica {
    ctx: ComponentContext<Self>,
    slot: Slot,
    index: u64,
    log: BufferMap,
    executed_watermark: u64,
    num_chosen: u64,
    recover_timer: Option<ScheduledTimer>,
    options: ReplicaOptions,
    num_replicas: u64,
    leaders: HashMap<u64, ActorPath>,
    leader_election_port: RequiredPort<LeaderElectionPort>,
    cached_client: ActorPath,
    current_leader: u64,
    current_round: u64,
}

impl Replica {
    pub(crate) fn with(
        pid: u64,
        options: ReplicaOptions,
        num_replicas: u64,
        leaders: HashMap<u64, ActorPath>,
        cached_client: ActorPath
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            slot: 0,
            index: pid,
            log: BufferMap::with(options.log_grow_size),
            executed_watermark: 0,
            num_chosen: 0,
            recover_timer: None,
            options,
            num_replicas,
            leaders,
            leader_election_port: RequiredPort::uninitialised(),
            cached_client,
            current_leader: 0,
            current_round: 0
        }
    }

    fn execute_log(&mut self) -> Vec<ProposalResp> {
        let mut responses = vec![];
        loop {
            match self.log.get(self.executed_watermark) {
                None => {
                    return responses;
                },
                Some(command_batch_or_noop) => {
                    let _slot = self.executed_watermark;
                    // ignore actual command execution
                    if self.current_leader == self.index {
                        match command_batch_or_noop {
                            CommandBatchOrNoop::CommandBatch(command_batch) => {
                                let mut resp = command_batch.iter().map(|data| ProposalResp::with(data.clone(), self.current_leader, self.current_round)).collect();
                                responses.append(&mut resp);
                            }
                            CommandBatchOrNoop::Noop => {}
                        }
                    }
                    self.executed_watermark = self.executed_watermark + 1;
                    let modulo = self.executed_watermark % self.options.send_chosen_watermark_every_n_entries;
                    let div = self.executed_watermark / self.options.send_chosen_watermark_every_n_entries;
                    if modulo == 0 && div % self.num_replicas == self.index {
                        for (_pid, leader) in &self.leaders {
                            let c = ChosenWatermark { slot: self.executed_watermark };
                            leader.tell_serialised(LeaderInbound::ChosenWatermark(c), self).expect("Failed to send message");
                        }
                    }
                },
            }
        }
    }

    fn handle_chosen(&mut self, chosen: Chosen) {
        let is_recover_timer_running = self.num_chosen != self.executed_watermark;
        let old_executed_watermark = self.executed_watermark;

        match self.log.get(chosen.slot) {
            Some(_) => {
                // We've already received a Chosen message for this slot. We ignore the message.
                return;
            },
            None => {
                self.log.put(chosen.slot, chosen.command_batch_or_noop);
                self.num_chosen = self.num_chosen + 1;
            }
        }
        let client_reply_batch = self.execute_log();
        for reply in client_reply_batch {
            self.cached_client.tell_serialised(AtomicBroadcastMsg::ProposalResp(reply), self).expect("Failed to send message");
        }

        let should_recover_timer_be_running = self.num_chosen != self.executed_watermark;
        let should_recover_timer_be_reset = old_executed_watermark != self.executed_watermark;
        if is_recover_timer_running {
            match (should_recover_timer_be_running, should_recover_timer_be_reset) {
                (true, true) => self.reset_recover_timer(),
                (true, false) => {},    // Do nothing.
                (false, _) => self.stop_recover_timer(),
            }
        } else if should_recover_timer_be_running {
            self.start_recover_timer();
        }

    }

    fn start_recover_timer(&mut self) {
        todo!()
    }

    fn stop_recover_timer(&mut self) {
        if let Some(timer) = std::mem::take(&mut self.recover_timer) {
            self.cancel_timer(timer);
        }
    }

    fn reset_recover_timer(&mut self) {
        self.stop_recover_timer();
        self.start_recover_timer();
    }
}

impl Actor for Replica {
    type Message = ();

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        Handled::Ok
    }

    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        let NetMessage { data, .. } = msg;
        match_deser! {data {
            msg(r): ReplicaInbound [using ReplicaInboundSer] => {
                match r {
                    ReplicaInbound::Chosen(c) => self.handle_chosen(c),
                }
            },
            err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            default(_) => unimplemented!("Should be Replica message!")
        }
        }
        Handled::Ok
    }
}

impl ComponentLifecycle for Replica {
    fn on_start(&mut self) -> Handled {
        todo!()
    }

    fn on_kill(&mut self) -> Handled {
        todo!()
    }
}


impl Require<LeaderElectionPort> for Replica {
    fn handle(&mut self, event: <LeaderElectionPort as Port>::Indication) -> Handled {
        todo!()
    }
}
