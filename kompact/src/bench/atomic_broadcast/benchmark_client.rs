extern crate raft as tikv_raft;

use super::{
    super::*,
    paxos::PaxosComp,
    raft::RaftComp,
    storage::paxos::{MemorySequence, MemoryState},
};
use crate::bench::atomic_broadcast::{
    paxos::{LeaderElection, PaxosCompMsg},
    raft::RaftCompMsg,
    util::exp_util::*,
};
use kompact::prelude::*;
use std::sync::Arc;
use tikv_raft::storage::MemStorage;

#[derive(Debug, Clone)]
pub struct ClientParams {
    pub(crate) experiment_str: String,
}

impl ClientParams {
    pub(crate) fn with(experiment_str: String) -> ClientParams {
        ClientParams { experiment_str }
    }
}

#[derive(Debug, Clone)]
struct ClientParamsDeser {
    pub algorithm: String,
    pub num_nodes: u64,
    pub is_reconfig_exp: bool,
}

impl ClientParamsDeser {
    fn with(algorithm: String, num_nodes: u64, is_reconfig_exp: bool) -> ClientParamsDeser {
        ClientParamsDeser {
            algorithm,
            num_nodes,
            is_reconfig_exp,
        }
    }
}

fn get_deser_clientparams_and_subdir(s: &str) -> (ClientParamsDeser, String) {
    let split: Vec<_> = s.split(',').collect();
    assert_eq!(split.len(), 7);
    let algorithm = split[0].to_lowercase();
    let num_nodes = split[1]
        .parse::<u64>()
        .unwrap_or_else(|_| panic!("{}' does not represent a node id", split[1]));
    let concurrent_proposals = split[2]
        .parse::<u64>()
        .unwrap_or_else(|_| panic!("{}' does not represent concurrent proposals", split[2]));
    let reconfig = split[4].to_lowercase();
    let is_reconfig_exp = match reconfig.as_str() {
        "off" => false,
        r if r == "single" || r == "majority" => true,
        other => panic!("Got unexpected reconfiguration: {}", other),
    };
    let cp = ClientParamsDeser::with(algorithm, num_nodes, is_reconfig_exp);
    let subdir = create_metaresults_sub_dir(num_nodes, concurrent_proposals, reconfig.as_str());
    (cp, subdir)
}

pub struct AtomicBroadcastClient {
    system: Option<KompactSystem>,
    paxos_comp: Option<Arc<Component<PaxosComp<MemorySequence, MemoryState>>>>,
    raft_comp: Option<Arc<Component<RaftComp<MemStorage>>>>,
}

impl AtomicBroadcastClient {
    pub fn new() -> AtomicBroadcastClient {
        AtomicBroadcastClient {
            system: None,
            paxos_comp: None,
            raft_comp: None,
        }
    }
}

impl DistributedBenchmarkClient for AtomicBroadcastClient {
    type ClientConf = ClientParams;
    type ClientData = ActorPath;

    fn setup(&mut self, c: Self::ClientConf) -> Self::ClientData {
        println!("Setting up Atomic Broadcast (client)");
        let mut conf = KompactConfig::default();
        conf.load_config_file(CONFIG_PATH);
        let bc = BufferConfig::from_config_file(CONFIG_PATH);
        bc.validate();
        let system = crate::kompact_system_provider::global()
            .new_remote_system_with_threads_config("atomicbroadcast", 8, conf, bc, TCP_NODELAY);
        let (params, meta_subdir) = get_deser_clientparams_and_subdir(&c.experiment_str);
        let experiment_params =
            ExperimentParams::load_from_file(CONFIG_PATH, meta_subdir, c.experiment_str);
        let initial_config: Vec<u64> = (1..=params.num_nodes).collect();
        let named_path = match params.algorithm.as_ref() {
            a if a == "paxos" || a == "vr" || a == "multi-paxos" => {
                let leader_election = match a {
                    "vr" => LeaderElection::VR,
                    "multi-paxos" => LeaderElection::MultiPaxos,
                    _ => LeaderElection::BLE,
                };
                let (paxos_comp, unique_reg_f) = system.create_and_register(|| {
                    PaxosComp::with(
                        initial_config,
                        params.is_reconfig_exp,
                        experiment_params,
                        leader_election,
                    )
                });
                unique_reg_f.wait_expect(REGISTER_TIMEOUT, "ReplicaComp failed to register!");
                let self_path = system
                    .register_by_alias(&paxos_comp, PAXOS_PATH)
                    .wait_expect(REGISTER_TIMEOUT, "Failed to register alias for ReplicaComp");
                let paxos_comp_f = system.start_notify(&paxos_comp);
                paxos_comp_f
                    .wait_timeout(REGISTER_TIMEOUT)
                    .expect("ReplicaComp never started!");
                self.paxos_comp = Some(paxos_comp);
                self_path
            }
            r if r == "raft" || r == "raft_pv_qc" => {
                let pv_qc = r == "raft_pv_qc";
                /*** Setup RaftComp ***/
                let (raft_comp, unique_reg_f) = system.create_and_register(|| {
                    RaftComp::<MemStorage>::with(initial_config, experiment_params, pv_qc)
                });
                unique_reg_f.wait_expect(REGISTER_TIMEOUT, "RaftComp failed to register!");
                let self_path = system
                    .register_by_alias(&raft_comp, RAFT_PATH)
                    .wait_expect(REGISTER_TIMEOUT, "Communicator failed to register!");
                let raft_comp_f = system.start_notify(&raft_comp);
                raft_comp_f
                    .wait_timeout(REGISTER_TIMEOUT)
                    .expect("RaftComp never started!");

                self.raft_comp = Some(raft_comp);
                self_path
            }
            unknown => panic!("Got unknown algorithm: {}", unknown),
        };
        self.system = Some(system);
        println!("Got path for Atomic Broadcast actor: {}", named_path);
        named_path
    }

    fn prepare_iteration(&mut self) -> () {
        println!("Preparing Atomic Broadcast (client)");
    }

    fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
        println!("Cleaning up Atomic Broadcast (client)");
        if let Some(paxos) = &self.paxos_comp {
            let kill_comps_f = paxos
                .actor_ref()
                .ask_with(|p| PaxosCompMsg::KillComponents(Ask::new(p, ())));
            kill_comps_f.wait();
        }
        if let Some(raft) = &self.raft_comp {
            let kill_comps_f = raft
                .actor_ref()
                .ask_with(|p| RaftCompMsg::KillComponents(Ask::new(p, ())));
            kill_comps_f.wait();
        }
        println!("KillAsk complete");
        if last_iteration {
            let system = self.system.take().unwrap();
            if let Some(replica) = self.paxos_comp.take() {
                let kill_replica_f = system.kill_notify(replica);
                kill_replica_f
                    .wait_timeout(REGISTER_TIMEOUT)
                    .expect("Paxos Replica never died!");
            }
            if let Some(raft_replica) = self.raft_comp.take() {
                let kill_raft_f = system.kill_notify(raft_replica);
                kill_raft_f
                    .wait_timeout(REGISTER_TIMEOUT)
                    .expect("Raft Replica never died!");
            }
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
        }
    }
}
