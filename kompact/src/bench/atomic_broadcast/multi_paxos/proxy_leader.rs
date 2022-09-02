use std::time::Duration;
use hashbrown::HashMap;
use kompact::prelude::*;
use crate::bench::atomic_broadcast::multi_paxos::messages::{AcceptorInbound, Chosen, Phase2a, Phase2b, ProxyLeaderInbound, ReplicaInbound};
use super::serializers::ProxyLeaderInboundSer;

type AcceptorIndex = u64;

#[derive(Eq, Hash, PartialEq)]
struct SlotRound {
    slot: u64,
    round: u64
}

struct Pending {
    phase2a: Phase2a,
    phase2bs: HashMap<AcceptorIndex, Phase2b>
}

enum State {
    Pending(Pending),
    Done
}

#[derive(ComponentDefinition)]
pub struct ProxyLeader {
    ctx: ComponentContext<Self>,
    acceptors: HashMap<u64, ActorPath>,
    replicas: HashMap<u64, ActorPath>,
    states: HashMap<SlotRound, State>,
    majority: usize,
}

impl ProxyLeader {
    pub(crate) fn with(
        acceptors: HashMap<u64, ActorPath>,
        replicas: HashMap<u64, ActorPath>,
    ) -> Self {
        let majority = acceptors.len()/2 + 1;
        Self {
            ctx: ComponentContext::uninitialised(),
            acceptors,
            replicas,
            states: HashMap::new(),
            majority,
        }
    }

    fn handle_phase2a(&mut self, phase2a: Phase2a) {
        let slotround = SlotRound { slot: phase2a.slot, round: phase2a.round };
        match self.states.get_mut(&slotround) {
            Some(_) => {
                debug!(
                    self.ctx.log(),
                    "A ProxyLeader received a Phase2a in slot {:?} and round {:?}, but it already received this Phase2a. The ProxyLeader is ignoring the message.",
                    phase2a.slot,
                    phase2a.round
                )
            }
            None => {
                for (_pid, acceptor) in &self.acceptors {
                    acceptor.tell_serialised(AcceptorInbound::Phase2a(phase2a.clone()), self).expect("Failed to send message");
                }

                // Update our state.
                self.states.insert(slotround, State::Pending(Pending{ phase2a, phase2bs: HashMap::new() }));
            }
        }
    }

    fn handle_phase2b(&mut self, phase2b: Phase2b) {
        let slotround = SlotRound { slot: phase2b.slot, round: phase2b.round };
        let chosen = match self.states.get_mut(&slotround) {    // needed to introduce this due to Rust's ownership rules
            None => {
                error!(
                    self.ctx.log(),
                    "A ProxyLeader received a Phase2b in slot {:?} and round {:?}, but it never sent a Phase2a in this round.",
                    phase2b.slot,
                    phase2b.round
                );
                return;
            }
            Some(State::Done) => {
                debug!(
                    self.ctx.log(),
                    "A ProxyLeader received a Phase2b in slot {:?} and round {:?}, but it has already chosen a value in this slot and round. The Phase2b is ignored.",
                    phase2b.slot,
                    phase2b.round
                );
                return;
            }
            Some(State::Pending(pending)) => {
                let phase2bs = &mut pending.phase2bs;
                let phase2b_slot = phase2b.slot;
                phase2bs.insert(phase2b.acceptor_index, phase2b);
                if phase2bs.len() < self.majority {
                    return;
                }
                Chosen { slot: phase2b_slot, command_batch_or_noop: pending.phase2a.command_batch_or_noop.clone() }
            }
        };
        // Let the replicas know that the value has been chosen.
        for (_pid, replica) in &self.replicas {
            replica.tell_serialised(ReplicaInbound::Chosen(chosen.clone()), self).expect("Failed to send message");
        }
        self.states.insert(slotround, State::Done);
    }
}

impl Actor for ProxyLeader {
    type Message = ProxyLeaderInbound;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            ProxyLeaderInbound::Phase2a(p2a) => self.handle_phase2a(p2a),
            _ => unimplemented!("Local message should only be Phase2a")
        }
        Handled::Ok
    }

    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        let NetMessage { data, .. } = msg;
        match_deser! {data {
            msg(l): ProxyLeaderInbound [using ProxyLeaderInboundSer] => {
                match l {
                    ProxyLeaderInbound::Phase2b(p2b) => self.handle_phase2b(p2b),
                    _ => unimplemented!("Remote message should only be Phase2b")
                }
            },
            err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            default(_) => unimplemented!("Should be ProxyLeaderInbound message!")
        }
        }
        Handled::Ok
    }
}

impl ComponentLifecycle for ProxyLeader {
    fn on_start(&mut self) -> Handled {
        todo!()
    }

    fn on_kill(&mut self) -> Handled {
        todo!()
    }
}