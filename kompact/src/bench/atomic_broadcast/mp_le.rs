#[cfg(feature = "measure_io")]
use crate::bench::atomic_broadcast::util::io_metadata::IOMetaData;
use crate::bench::atomic_broadcast::{
    ble::{Ballot, BallotLeaderElection, Stop},
    messages::{
        paxos::mp_leader_election::{MPLeaderSer, *},
        StopMsg as NetStopMsg, StopMsgDeser,
    },
    mp_le::State::Follower,
};
use hashbrown::HashSet;
use kompact::prelude::*;
use omnipaxos::leader_election::Leader;
use rand::{prelude::Rng, thread_rng};
use std::time::Duration;

pub struct ElectionOptions {
    ping_period: Duration,
    timeout_min: Duration,
    timeout_max: Duration,
}

impl ElectionOptions {
    fn get_random_timeout_period(&self) -> Duration {
        let mut rng = thread_rng();
        let min = self.timeout_min.as_millis();
        let max = self.timeout_max.as_millis();
        let rnd: u128 = rng.gen_range(min, max);
        Duration::from_millis(rnd as u64)
    }
}

#[derive(Copy, Clone)]
enum State {
    Leader,
    Follower,
}

/// The `MultiPaxosLeaderComp` is an identical implementation of the `Participant` class in the leader election of frankenpaxos:
/// https://github.com/mwhittaker/frankenpaxos/blob/master/shared/src/main/scala/frankenpaxos/election/basic/Participant.scala
#[derive(ComponentDefinition)]
pub struct MultiPaxosLeaderComp {
    ctx: ComponentContext<Self>,
    /*** Variables directly equivalent to the ones with the same name in the original `Participant` implementation ***/
    callback: ProvidedPort<BallotLeaderElection>,
    index: u64,
    pub(crate) other_participants: Vec<ActorPath>,
    round: u64,
    leader_index: u64,
    ping_timer: Option<ScheduledTimer>,
    no_ping_timer: Option<ScheduledTimer>,
    state: State,
    election_options: ElectionOptions,
    /*** Experiment-specific variables ***/
    stopped: bool,
    stopped_peers: HashSet<u64>,
    stop_ask: Option<Ask<u64, ()>>,
    #[cfg(feature = "measure_io")]
    io_metadata: IOMetaData,
    #[cfg(feature = "simulate_partition")]
    disconnected_peers: Vec<u64>,
}

impl MultiPaxosLeaderComp {
    pub fn with(
        peers: Vec<ActorPath>,
        pid: u64,
        initial_leader_index: u64,
        election_options: ElectionOptions,
    ) -> MultiPaxosLeaderComp {
        let index = pid;
        let state = if index == initial_leader_index {
            State::Leader
        } else {
            State::Follower
        };
        MultiPaxosLeaderComp {
            ctx: ComponentContext::uninitialised(),
            callback: ProvidedPort::uninitialised(),
            index,
            other_participants: peers,
            round: 0,
            leader_index: 0,
            ping_timer: None,
            no_ping_timer: None,
            state,
            election_options,
            stopped: false,
            stopped_peers: HashSet::new(),
            stop_ask: None,
            #[cfg(feature = "measure_io")]
            io_metadata: IOMetaData::default(),
            #[cfg(feature = "simulate_partition")]
            disconnected_peers: vec![],
        }
    }

    fn start_ping_timer(&mut self) {
        let ping_period = self.election_options.ping_period;
        let ping_timer = self.schedule_periodic(ping_period, ping_period, move |c, _| {
            c.ping();
            Handled::Ok
        });
        self.ping_timer = Some(ping_timer);
    }

    fn ping(&self) {
        for peer in &self.other_participants {
            peer.tell_serialised(Ping::with(self.round, self.leader_index), self)
                .expect("Ping should serialise!");
        }
    }

    fn stop_ping_timer(&mut self) {
        if let Some(ping_timer) = std::mem::take(&mut self.ping_timer) {
            self.cancel_timer(ping_timer);
        }
    }

    fn start_no_ping_timer(&mut self) {
        let random_period = self.election_options.get_random_timeout_period();
        let no_ping_timer = self.schedule_once(random_period, move |c, _| {
            c.round = c.round + 1;
            c.leader_index = c.index;
            c.change_state(State::Leader);
            Handled::Ok
        });
        self.no_ping_timer = Some(no_ping_timer);
    }

    fn stop_no_ping_timer(&mut self) {
        if let Some(no_ping_timer) = std::mem::take(&mut self.no_ping_timer) {
            self.cancel_timer(no_ping_timer);
        }
    }

    fn reset_no_ping_timer(&mut self) {
        self.stop_no_ping_timer();
        self.start_no_ping_timer();
    }

    fn change_state(&mut self, new_state: State) {
        match (self.state, new_state) {
            (State::Leader, State::Leader) => {}     // Do nothing.
            (State::Follower, State::Follower) => {} // Do nothing.
            (State::Follower, State::Leader) => {
                self.stop_no_ping_timer();
                self.start_ping_timer();
                self.state = State::Leader;
                self.ping();
                let ballot = Ballot::with(self.round as u32, self.leader_index); // create a ballot from the elected leader as our callback interface require it.
                self.callback
                    .trigger(Leader::with(self.leader_index, ballot));
            }
            (State::Leader, State::Follower) => {
                self.stop_ping_timer();
                self.start_no_ping_timer();
                self.state = Follower;
                let ballot = Ballot::with(self.round as u32, self.leader_index); // create a ballot from the elected leader as our callback interface require it.
                self.callback
                    .trigger(Leader::with(self.leader_index, ballot));
            }
        }
    }

    fn handle_ping(&mut self, ping: Ping) {
        let ping_ballot = (ping.round, ping.leader_index);
        let ballot = (self.round, self.index);
        match self.state {
            State::Follower => {
                if ping_ballot < ballot {
                    debug!(
                        self.ctx().log(),
                        "A participant received a stale Ping message in round {} with leader but is already {:?} in round {} with leader {}. The ping is being ignored",
                        ping.round, ping.leader_index, self.round, self.leader_index
                    );
                } else if ping_ballot == ballot {
                    self.reset_no_ping_timer();
                } else {
                    self.round = ping.round;
                    self.leader_index = ping.leader_index;
                    self.reset_no_ping_timer();
                }
            }
            State::Leader => {
                if ping_ballot <= ballot {
                    debug!(
                        self.ctx().log(),
                        "A participant received a stale Ping message in round {} with leader but is already {:?} in round {} with leader {}. The ping is being ignored",
                        ping.round, ping.leader_index, self.round, self.leader_index
                    );
                } else {
                    self.round = ping.round;
                    self.leader_index = ping.leader_index;
                    self.change_state(State::Follower);
                }
            }
        }
    }

    #[cfg(feature = "simulate_partition")]
    pub fn disconnect_peers(&mut self, peers: Vec<u64>, lagging_peer: Option<u64>) {
        // info!(self.ctx.log(), "SIMULATE PARTITION: {:?}, lagging: {:?}", peers, lagging_peer);
        if let Some(lp) = lagging_peer {
            // disconnect from lagging peer first
            self.disconnected_peers.push(lp);
            let a = peers.clone();
            let lagging_delay = self.ctx.config()["partition_experiment"]["lagging_delay"]
                .as_duration()
                .expect("No lagging duration!");
            self.schedule_once(lagging_delay, move |c, _| {
                for pid in a {
                    c.disconnected_peers.push(pid);
                }
                Handled::Ok
            });
        } else {
            self.disconnected_peers = peers;
        }
    }

    #[cfg(feature = "simulate_partition")]
    pub fn recover_peers(&mut self) {
        self.disconnected_peers.clear();
    }

    #[cfg(feature = "measure_io")]
    pub fn get_io_metadata(&mut self) -> IOMetaData {
        self.io_metadata
    }
}

impl ComponentLifecycle for MultiPaxosLeaderComp {
    fn on_start(&mut self) -> Handled {
        match self.state {
            State::Leader => self.start_ping_timer(),
            State::Follower => self.start_no_ping_timer(),
        }
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        self.stop_ping_timer();
        self.stop_no_ping_timer();
        Handled::Ok
    }
}

impl Provide<BallotLeaderElection> for MultiPaxosLeaderComp {
    fn handle(&mut self, _: <BallotLeaderElection as Port>::Request) -> Handled {
        unimplemented!()
    }
}

impl Actor for MultiPaxosLeaderComp {
    type Message = Stop;

    fn receive_local(&mut self, stop: Stop) -> Handled {
        #[cfg(feature = "simulate_partition")]
        {
            self.disconnected_peers.clear();
        }
        let pid = stop.0.request();
        self.stop_ping_timer();
        self.stop_no_ping_timer();
        for peer in &self.other_participants {
            peer.tell_serialised(NetStopMsg::Peer(*pid), self)
                .expect("NetStopMsg should serialise!");
        }
        self.stopped = true;
        if self.stopped_peers.len() == self.other_participants.len() {
            stop.0.reply(()).expect("Failed to reply to stop ask!");
        } else {
            self.stop_ask = Some(stop.0);
        }
        Handled::Ok
    }

    fn receive_network(&mut self, m: NetMessage) -> Handled {
        let NetMessage { data, .. } = m;
        match_deser! {data {
            msg(ping): Ping [using MPLeaderSer] => {
                self.handle_ping(ping);
            },
            msg(stop): NetStopMsg [using StopMsgDeser] => {
                if let NetStopMsg::Peer(pid) = stop {
                    assert!(self.stopped_peers.insert(pid), "BLE got duplicate stop from peer {}", pid);
                    debug!(self.ctx.log(), "BLE got stopped from peer {}", pid);
                    if self.stopped && self.stopped_peers.len() == self.other_participants.len() {
                        debug!(self.ctx.log(), "BLE got stopped from all peers");
                        self.stop_ask
                            .take()
                            .expect("No stop ask!")
                            .reply(())
                            .expect("Failed to reply ask");
                    }
                }

            },
            err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            default(_) => unimplemented!("Should be either HeartbeatMsg or NetStopMsg!"),
        }
        }
        Handled::Ok
    }
}
