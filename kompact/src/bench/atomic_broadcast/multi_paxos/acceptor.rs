use std::cmp::max;
use std::collections::{BTreeMap};
use hashbrown::HashMap;
use kompact::prelude::*;
use crate::bench::atomic_broadcast::multi_paxos::messages::{AcceptorInbound, CommandBatchOrNoop, LeaderInbound, Nack, Phase1a, Phase1b, Phase1bSlotInfo, Phase2a, Phase2b, ProxyLeaderInbound};
use crate::bench::atomic_broadcast::multi_paxos::util::{Round, Slot};
use super::serializers::AcceptorInboundSer;

struct State {
    vote_round: Round,
    vote_value: CommandBatchOrNoop
}

#[derive(ComponentDefinition)]
pub struct Acceptor {
    ctx: ComponentContext<Self>,
    index: u64,
    round: Round,
    states: BTreeMap<Slot, State>,
    max_voted_slot: Slot,
    leaders: HashMap<u64, ActorPath>,
    proxy_leaders: HashMap<u64, ActorPath>
}

impl Acceptor {
    pub(crate) fn with(
        pid: u64,
        leaders: HashMap<u64, ActorPath>,
        proxy_leaders: HashMap<u64, ActorPath>
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            index: pid,
            round: 0,
            states: Default::default(),
            max_voted_slot: 0,
            leaders,
            proxy_leaders
        }
    }

    fn handle_phase1a(&mut self, phase1a: Phase1a) {
        let src = phase1a.src;
        if phase1a.round < self.round {
            debug!(self.ctx.log(), "An acceptor received a Phase1a message in round {:?} but is in round {:?}.", phase1a.round, self.round);
            let nack = Nack { round: self.round };
            let leader = self.leaders.get(&src).expect("No actorpath for leader");
            leader.tell_serialised(LeaderInbound::Nack(nack), self).expect("Failed to send message");
        }

        self.round = phase1a.round;
        let info = self.states.range(phase1a.chosen_watermark..).map(|(slot, state)| Phase1bSlotInfo {
            slot: *slot,
            vote_round: state.vote_round,
            vote_value: state.vote_value.clone()
        }).collect();
        let phase1b = Phase1b {
            acceptor_index: self.index,
            round: self.round,
            info
        };
        let leader = self.leaders.get(&src).expect("No actorpath for leader");
        leader.tell_serialised(LeaderInbound::Phase1b(phase1b), self).expect("Failed to send message");
    }

    fn handle_phase2a(&mut self, phase2a: Phase2a) {
        let src= phase2a.src;
        if phase2a.round < self.round {
            debug!(self.ctx.log(), "An acceptor received a Phase2a message in round {:?} but is in round {:?}.", phase2a.round, self.round);
            let nack = Nack { round: self.round };
            let leader = self.leaders.get(&src).expect("No actorpath for leader");
            leader.tell_serialised(LeaderInbound::Nack(nack), self).expect("Failed to send message");
        }
        self.round = phase2a.round;
        self.states.insert(phase2a.slot, State { vote_round: self.round, vote_value: phase2a.command_batch_or_noop });
        self.max_voted_slot = max(self.max_voted_slot, phase2a.slot);
        let phase2b = Phase2b {
            acceptor_index: self.index,
            slot: phase2a.slot,
            round: self.round
        };
        let proxy_leader = self.proxy_leaders.get(&src).expect("No actorpath for leader");
        proxy_leader.tell_serialised(ProxyLeaderInbound::Phase2b(phase2b), self).expect("Failed to send message");
    }
}

impl Actor for Acceptor {
    type Message = ();

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        Handled::Ok
    }

    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        let NetMessage { data, .. } = msg;
        match_deser! {data {
            msg(a): AcceptorInbound [using AcceptorInboundSer] => {
                match a {
                    AcceptorInbound::Phase1a(p1a) => self.handle_phase1a(p1a),
                    AcceptorInbound::Phase2a(p2a) => self.handle_phase2a(p2a),
                }
            },
            err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            default(_) => unimplemented!("Should be Acceptor message!")
        }
        }
        Handled::Ok
    }
}

impl ComponentLifecycle for Acceptor {
    fn on_start(&mut self) -> Handled {
        todo!()
    }

    fn on_kill(&mut self) -> Handled {
        todo!()
    }
}