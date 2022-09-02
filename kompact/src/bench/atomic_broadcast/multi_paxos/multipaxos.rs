use std::ops::DerefMut;
use hashbrown::HashMap;
use std::sync::Arc;
use std::time::Duration;
use kompact::prelude::*;
use crate::bench::atomic_broadcast::communicator::Communicator;
use crate::bench::atomic_broadcast::multi_paxos::acceptor::Acceptor;
use crate::bench::atomic_broadcast::multi_paxos::batcher::Batcher;
use crate::bench::atomic_broadcast::multi_paxos::leader::{Leader, LeaderOptions};
use crate::bench::atomic_broadcast::multi_paxos::participant::{ElectionOptions, Participant};
use crate::bench::atomic_broadcast::multi_paxos::proxy_leader::ProxyLeader;
use crate::bench::atomic_broadcast::multi_paxos::replica::{Replica, ReplicaOptions};
use crate::bench::atomic_broadcast::multi_paxos::util::LeaderElectionPort;
use crate::bench::atomic_broadcast::util::exp_util::ExperimentParams;
use crate::partitioning_actor::PartitioningActorMsg;

const PARTICIPANT: &str = "participant";
const LEADER: &str = "leader";
const ACCEPTOR: &str = "acceptor";
const REPLICA: &str = "replica";
const PROXY_LEADER: &str = "proxy_leader";

struct AllActorPaths {
    participant_peers: Vec<ActorPath>,
    leaders: HashMap<u64, ActorPath>,
    acceptors: HashMap<u64, ActorPath>,
    replicas: HashMap<u64, ActorPath>,
    proxy_leaders: HashMap<u64, ActorPath>,
}

#[derive(ComponentDefinition)]
pub struct MultiPaxosComp {
    ctx: ComponentContext<Self>,
    pid: u64,
    initial_nodes: Vec<u64>,
    nodes: Vec<ActorPath>, // derive actorpaths of peers' leader, acceptor, replica, and participant
    communicator_comps: Vec<Arc<Component<Communicator>>>,
    leader_comp: Option<Arc<Component<Leader>>>,
    acceptor_comp: Option<Arc<Component<Acceptor>>>,
    replica_comp: Option<Arc<Component<Replica>>>,
    participant_comp: Option<Arc<Component<Participant>>>,
    proxy_leader_comp: Option<Arc<Component<ProxyLeader>>>,
    batcher_comp: Option<Arc<Component<Batcher>>>,
    iteration_id: u32,
    stopped: bool,
    partitioning_actor: Option<ActorPath>,
    cached_client: Option<ActorPath>,
    current_leader: u64,
    experiment_params: ExperimentParams,
}

impl MultiPaxosComp {
    pub fn with(
        initial_nodes: Vec<u64>,
        experiment_params: ExperimentParams,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            pid: 0,
            initial_nodes,
            nodes: vec![],
            communicator_comps: vec![],
            leader_comp: None,
            acceptor_comp: None,
            replica_comp: None,
            participant_comp: None,
            proxy_leader_comp: None,
            batcher_comp: None,
            iteration_id: 0,
            stopped: false,
            partitioning_actor: None,
            cached_client: None,
            current_leader: 0,
            experiment_params
        }
    }

    fn reset_state(&mut self) {
        self.pid = 0;
        self.nodes.clear();
        self.iteration_id = 0;
        self.stopped = false;
        self.current_leader = 0;
        self.cached_client = None;
        self.partitioning_actor = None;

        self.leader_comp = None;
        self.acceptor_comp = None;
        self.replica_comp = None;
        self.participant_comp = None;
        self.communicator_comps.clear();
    }

    fn create_options(&self) -> (ElectionOptions, LeaderOptions, ReplicaOptions) {
        todo!()
        /*
        let config = self.ctx.config();
        // participant params
        let timeout_delta = config["multipaxos"]["timeout_delta"].as_i64().unwrap() as u64;

        // leader params
        let resend_phase1as_period = config["multipaxos"]["resend_phase1as_period"].as_duration().unwrap();
        let flush_phase2as_every_n = config["multipaxos"]["flush_phase2as_every_n"].as_i64().unwrap() as u64;
        let noop_flush_period = config["multipaxos"]["noop_flush_period"].as_duration().unwrap();

        // replica params
        let log_grow_size = config["multipaxos"]["log_grow_size"].as_i64().unwrap() as usize;
        let send_chosen_watermark_every_n_entries = config["multipaxos"]["send_chosen_watermark_every_n_entries"].as_i64().unwrap() as u64;
        let recover_log_entry_min_period = config["multipaxos"]["recover_log_entry_min_period"].as_duration().unwrap();
        let recover_log_entry_max_period = config["multipaxos"]["recover_log_entry_min_period"].as_duration().unwrap();

        let election_opts = ElectionOptions {
            ping_period: Duration::from_millis(self.experiment_params.election_timeout),
            timeout_min: Duration::from_millis(self.experiment_params.election_timeout - timeout_delta),
            timeout_max: Duration::from_millis(self.experiment_params.election_timeout + timeout_delta),
        };
        let leader_opts = LeaderOptions { resend_phase1as_period, flush_phase2as_every_n, noop_flush_period };
        let replica_opts = ReplicaOptions { log_grow_size, send_chosen_watermark_every_n_entries, recover_log_entry_min_period, recover_log_entry_max_period };

        (election_opts, leader_opts, replica_opts)
    }

    fn create_components(&mut self) {
        let system = self.ctx.system();
        let client_path = self.cached_client.expect("No cached client");
        let AllActorPaths {
            participant_peers,
            leaders,
            acceptors,
            replicas,
            proxy_leaders
        } = self.derive_actorpaths(&self.initial_nodes);
        let (election_opts, leader_opts, replica_opts) = self.create_options();
        let initial_leader = self.nodes.len() as u64;
        let (participant, participant_f) = system.create_and_register(|| {
            Participant::with(participant_peers, self.pid, initial_leader, election_opts)
        });
        let (proxy_leader, proxy_leader_f) = system.create_and_register(|| {
            ProxyLeader::with(acceptors.clone(), replicas)
        });
        let (leader, leader_f) = system.create_and_register(|| {
            Leader::with(self.pid, leader_opts, acceptors, proxy_leader.actor_ref())
        });
        let (acceptor, acceptor_f) = system.create_and_register(|| {
            Acceptor::with(self.pid, leaders.clone(), proxy_leaders)
        });
        let (replica, replica_f) = system.create_and_register(|| {
            Replica::with(self.pid, replica_opts, self.nodes.len() as u64, leaders.clone(), client_path)
        });
        let (batcher, batcher_f) = system.create_and_register(|| {
            Batcher::with(leaders)
        });

        let futures = [participant_f, proxy_leader_f, leader_f, acceptor_f, replica_f, batcher_f];

        biconnect_components::<LeaderElectionPort, _, _>(&participant, &leader).expect("Could not connect Participant and Leader");
        biconnect_components::<LeaderElectionPort, _, _>(&participant, &replica).expect("Could not connect Participant and Replica");

        self.participant_comp = Some(participant);
        self.leader_comp = Some(leader);
        self.acceptor_comp = Some(acceptor);
        self.replica_comp = Some(replica);
        self.proxy_leader_comp = Some(proxy_leader);
        self.batcher_comp = Some(batcher);

        Handled::block_on(self, move |mut async_self| async move {
            for f in futures {
                f.await.unwrap().expect("Failed to register when creating components");
            }
            self.register_aliases();
            async_self
                .partitioning_actor
                .take()
                .expect("No partitioning actor found!")
                .tell_serialised(
                    PartitioningActorMsg::InitAck(async_self.iteration_id),
                    async_self.deref_mut(),
                )
                .expect("Should serialise InitAck");
        };
        */
    }

    fn register_aliases(&mut self) {
        todo!()
    }

    fn get_actorpath(&self, pid: u64) -> &ActorPath {
        let idx = pid as usize - 1;
        self.nodes
            .get(idx)
            .unwrap_or_else(|| panic!("Could not get actorpath of pid: {}", pid))
    }

    fn derive_component_actorpath(&self, n: &NamedPath, str: &str) -> ActorPath {
        let sys_path = n.system();
        let protocol = sys_path.protocol();
        let port = sys_path.port();
        let addr = sys_path.address();
        let named_path = NamedPath::new(
            protocol,
            *addr,
            port,
            vec![format!(
                "{}{}-{}",
                str, self.pid, self.iteration_id
            )],
        );
        ActorPath::Named(named_path)
    }

    fn derive_actorpaths(
        &self,
        nodes: &[u64],
    ) -> AllActorPaths {
        let num_nodes = nodes.len();
        let mut leaders = HashMap::with_capacity(num_nodes);
        let mut acceptors = HashMap::with_capacity(num_nodes);
        let mut replicas = HashMap::with_capacity(num_nodes);
        let mut proxy_leaders = HashMap::with_capacity(num_nodes);
        let mut participant_peers = Vec::with_capacity(num_nodes);
        for pid in nodes {
            let actorpath = self.get_actorpath(*pid);
            match actorpath {
                ActorPath::Named(n) => {
                    let named_leader = self.derive_component_actorpath(n, LEADER);
                    let named_acceptor = self.derive_component_actorpath(n, ACCEPTOR);
                    let named_replica = self.derive_component_actorpath(n, REPLICA);
                    let named_participant = self.derive_component_actorpath(n, PARTICIPANT);
                    let named_proxy_leader = self.derive_component_actorpath(PROXY_LEADER);

                    leaders.insert(*pid, named_leader);
                    acceptors.insert(*pid, named_acceptor);
                    replicas.insert(*pid, named_replica);
                    proxy_leaders.insert(*pid, named_proxy_leader);
                    if pid != self.pid {    // participant only need peers
                        participant_peers.push(named_participant);
                    }
                }
                _ => error!(
                    self.ctx.log(),
                    "{}",
                    format!("Actorpath is not named for node {}", pid)
                ),
            }
        }
        AllActorPaths {
            participant_peers,
            leaders,
            acceptors,
            replicas,
            proxy_leaders
        }
    }
}