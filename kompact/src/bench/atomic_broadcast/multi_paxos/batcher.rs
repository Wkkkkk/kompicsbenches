use std::time::Duration;
use hashbrown::HashMap;
use kompact::prelude::*;
use crate::bench::atomic_broadcast::messages::Proposal;
use crate::bench::atomic_broadcast::multi_paxos::messages::{BatcherInbound, LeaderInbound};
use crate::bench::atomic_broadcast::multi_paxos::util::{ClassicRoundRobin, Round};
use super::serializers::BatcherInboundSer;

#[derive(ComponentDefinition)]
pub struct Batcher {
    ctx: ComponentContext<Self>,
    growing_batch: Vec<Proposal>,
    pending_resend_batches: Vec<Vec<Proposal>>,
    leaders: HashMap<u64, ActorPath>,
    round: Round,
    round_system: ClassicRoundRobin,
    batch_timer: Option<ScheduledTimer>,
    // batcher_port: ProvidedPort<BatcherPort>
}

impl Batcher {
    pub fn with(
        leaders: HashMap<u64, ActorPath>,
    ) -> Self {
        let n = leaders.len() as u64;
        Self {
            ctx: ComponentContext::uninitialised(),
            growing_batch: vec![],
            pending_resend_batches: vec![],
            leaders,
            round: 0,
            round_system: ClassicRoundRobin { n },
            batch_timer: None
        }
    }

    fn handle_not_leader_batcher(&mut self, notleader_client_request_batch: Vec<Proposal>) {
        self.pending_resend_batches.push(notleader_client_request_batch);
    }

    fn handle_leader_info(&mut self, leader_info_round: Round) {
        if leader_info_round <= self.round {
            debug!(
                self.ctx.log(),
                "A batcher received a LeaderInfoReplyBatcher message with round {:?} but is already in round {:?}. The LeaderInfoReplyBatcher message  must be stale, so we are ignoring it.",
                leader_info_round,
                self.round
            );
            return;
        }

        let old_round = self.round;
        let new_round = leader_info_round;
        self.round = leader_info_round;
        if self.round_system.leader(old_round) != self.round_system.leader(new_round) {
            let leader = self.leaders.get(&self.round_system.leader(new_round)).expect("Did not find actorpath for leader");
            for batch in std::mem::take(&mut self.pending_resend_batches) {
                leader.tell_serialised(LeaderInbound::ClientRequestBatch(batch), self).expect("Failed to send message");
            }
            // pending_resend_batches is cleared by std::mem::take
        }
    }

    fn start_batch_timer(&mut self) {
        let timer = self.schedule_periodic(Duration::from_millis(0), Duration::from_millis(1), |c, _| {
            let leader = c.leaders.get(&c.round_system.leader(c.round)).expect("No leader");
            leader.tell_serialised(LeaderInbound::ClientRequestBatch(std::mem::take(&mut c.growing_batch)), c).expect("Failed to send message");
            // growing_batch is cleared by std::mem::take
            Handled::Ok
        });
        self.batch_timer = Some(timer);
    }
}

impl Actor for Batcher {
    type Message = ();

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        Handled::Ok
    }

    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        let NetMessage { data, .. } = msg;
        match_deser! {data {
            msg(b): BatcherInbound [using BatcherInboundSer] => {
                match b {
                    BatcherInbound::NotLeaderBatcher(b) => self.handle_not_leader_batcher(b),
                    BatcherInbound::LeaderInfoReplyBatcher(l) => self.handle_leader_info(l),
                }
            },
            err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            default(_) => unimplemented!("Should be Batcher message!")
        }
        }
        Handled::Ok
    }
}

impl ComponentLifecycle for Batcher {
    fn on_start(&mut self) -> Handled {
        todo!()
    }

    fn on_kill(&mut self) -> Handled {
        todo!()
    }
}
