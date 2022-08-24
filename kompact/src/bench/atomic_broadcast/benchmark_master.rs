use crate::{
    bench::atomic_broadcast::{
        benchmark_client::ClientParams,
        client::{Client, LocalClientMessage, MetaResults},
        util::exp_util::*,
    },
    partitioning_actor::{IterationControlMsg, PartitioningActor},
};
use benchmark_suite_shared::{
    kompics_benchmarks::benchmarks::AtomicBroadcastRequest, BenchmarkError, DeploymentMetaData,
    DistributedBenchmarkMaster,
};
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use hdrhistogram::Histogram;
use hocon::HoconLoader;
use kompact::prelude::*;
use quanta::Instant;
use std::{
    fs::{create_dir_all, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use synchronoise::CountdownEvent;

#[derive(Copy, Clone, Debug)]
pub enum ReconfigurationPolicy {
    ReplaceLeader,
    ReplaceFollower,
}

#[derive(Copy, Clone, Debug)]
pub enum NetworkScenario {
    FullyConnected,
    QuorumLoss,
    ConstrainedElection,
    Chained,
    PeriodicFull,
}

pub struct AtomicBroadcastMaster {
    num_nodes: Option<u64>,
    num_nodes_needed: Option<u64>,
    num_proposals: Option<u64>,
    local_proposal_file: Option<String>,
    concurrent_proposals: Option<u64>,
    reconfiguration: Option<(ReconfigurationPolicy, Vec<u64>)>,
    network_scenario: Option<NetworkScenario>,
    system: Option<KompactSystem>,
    finished_latch: Option<Arc<CountdownEvent>>,
    iteration_id: u32,
    client_comp: Option<Arc<Component<Client<StoreCommand>>>>,
    partitioning_actor: Option<Arc<Component<PartitioningActor>>>,
    latency_hist: Option<Histogram<u64>>,
    num_timed_out: Vec<u64>,
    num_retried: Vec<u64>,
    experiment_str: Option<String>,
    meta_results_path: Option<String>,
    meta_results_sub_dir: Option<String>,
}

impl AtomicBroadcastMaster {
    pub(crate) fn new() -> AtomicBroadcastMaster {
        AtomicBroadcastMaster {
            num_nodes: None,
            num_nodes_needed: None,
            num_proposals: None,
            local_proposal_file: None,
            concurrent_proposals: None,
            reconfiguration: None,
            network_scenario: None,
            system: None,
            finished_latch: None,
            iteration_id: 0,
            client_comp: None,
            partitioning_actor: None,
            latency_hist: None,
            num_timed_out: vec![],
            num_retried: vec![],
            experiment_str: None,
            meta_results_path: None,
            meta_results_sub_dir: None,
        }
    }

    fn initialise_iteration(
        &self,
        nodes: Vec<ActorPath>,
        client: ActorPath,
        pid_map: Option<HashMap<ActorPath, u32>>,
    ) -> Arc<Component<PartitioningActor>> {
        let system = self.system.as_ref().unwrap();
        let prepare_latch = Arc::new(CountdownEvent::new(1));
        /*** Setup partitioning actor ***/
        let (partitioning_actor, unique_reg_f) = system.create_and_register(|| {
            PartitioningActor::with(
                prepare_latch.clone(),
                None,
                self.iteration_id,
                nodes,
                pid_map,
                None,
            )
        });
        unique_reg_f.wait_expect(
            Duration::from_millis(1000),
            "PartitioningComp failed to register!",
        );

        let partitioning_actor_f = system.start_notify(&partitioning_actor);
        partitioning_actor_f
            .wait_timeout(Duration::from_millis(1000))
            .expect("PartitioningComp never started!");
        let mut ser_client = Vec::<u8>::new();
        client
            .serialise(&mut ser_client)
            .expect("Failed to serialise ClientComp actorpath");
        partitioning_actor
            .actor_ref()
            .tell(IterationControlMsg::Prepare(Some(ser_client)));
        prepare_latch.wait();
        partitioning_actor
    }

    fn create_client(
        &self,
        nodes_id: HashMap<u64, ActorPath>,
        network_scenario: NetworkScenario,
        client_timeout: Duration,
        preloaded_log_size: u64,
        leader_election_latch: Arc<CountdownEvent>,
    ) -> (Arc<Component<Client<StoreCommand>>>, ActorPath) {
        let system = self.system.as_ref().unwrap();
        let finished_latch = self.finished_latch.clone().unwrap();
        /*** Setup client ***/
        let initial_config: Vec<_> = (1..=self.num_nodes.unwrap()).map(|x| x as u64).collect();
        let reconfig = self.reconfiguration.clone();
        let (client_comp, unique_reg_f) = system.create_and_register(|| {
            Client::with(
                initial_config,
                self.num_proposals.unwrap(),
                self.concurrent_proposals.unwrap(),
                self.local_proposal_file.as_ref().unwrap().clone(),
                nodes_id,
                network_scenario,
                reconfig,
                client_timeout,
                preloaded_log_size,
                leader_election_latch,
                finished_latch,
            )
        });
        unique_reg_f.wait_expect(REGISTER_TIMEOUT, "Client failed to register!");
        let client_comp_f = system.start_notify(&client_comp);
        client_comp_f
            .wait_timeout(REGISTER_TIMEOUT)
            .expect("ClientComp never started!");
        let client_path = system
            .register_by_alias(&client_comp, format!("client{}", &self.iteration_id))
            .wait_expect(REGISTER_TIMEOUT, "Failed to register alias for ClientComp");
        (client_comp, client_path)
    }

    fn validate_experiment_params(
        &mut self,
        c: &AtomicBroadcastRequest,
        num_clients: u32,
    ) -> Result<(), BenchmarkError> {
        if (num_clients as u64) < c.number_of_nodes {
            return Err(BenchmarkError::InvalidTest(format!(
                "Not enough clients: {}, Required: {}",
                num_clients, c.number_of_nodes
            )));
        }
        if c.concurrent_proposals > c.number_of_proposals {
            return Err(BenchmarkError::InvalidTest(format!(
                "Concurrent proposals: {} should be less or equal to number of proposals: {}",
                c.concurrent_proposals, c.number_of_proposals
            )));
        }
        match &c.algorithm.to_lowercase() {
            a if a != "paxos"
                && a != "raft"
                && a != "vr"
                && a != "multi-paxos"
                && a != "raft_pv_qc" =>
            {
                return Err(BenchmarkError::InvalidTest(format!(
                    "Unimplemented atomic broadcast algorithm: {}",
                    &c.algorithm
                )));
            }
            _ => {}
        }
        match &c.reconfiguration.to_lowercase() {
            off if off == "off" => {
                self.num_nodes_needed = Some(c.number_of_nodes);
                if c.reconfig_policy.to_lowercase() != "none" {
                    return Err(BenchmarkError::InvalidTest(format!(
                        "Reconfiguration is off, transfer policy should be none, but found: {}",
                        &c.reconfig_policy
                    )));
                }
            }
            s if s == "single" || s == "majority" => {
                let policy = match c.reconfig_policy.to_lowercase().as_str() {
                    "replace-follower" => ReconfigurationPolicy::ReplaceFollower,
                    "replace-leader" => ReconfigurationPolicy::ReplaceLeader,
                    _ => {
                        return Err(BenchmarkError::InvalidTest(format!(
                            "Unimplemented reconfiguration policy: {}",
                            &c.reconfig_policy
                        )));
                    }
                };
                match get_reconfig_nodes(&c.reconfiguration, c.number_of_nodes) {
                    Ok(reconfig) => {
                        let additional_n = match &reconfig {
                            Some(r) => r.len() as u64,
                            None => 0,
                        };
                        let n = c.number_of_nodes + additional_n;
                        if (num_clients as u64) < n {
                            return Err(BenchmarkError::InvalidTest(format!(
                                "Not enough clients: {}, Required: {}",
                                num_clients, n
                            )));
                        }
                        self.reconfiguration = reconfig.map(|r| (policy, r));
                        self.num_nodes_needed = Some(n);
                    }
                    Err(e) => return Err(e),
                }
            }
            _ => {
                return Err(BenchmarkError::InvalidTest(format!(
                    "Unimplemented reconfiguration: {}",
                    &c.reconfiguration
                )));
            }
        }
        let network_scenario = match c.network_scenario.to_lowercase().as_str() {
            "fully_connected" => NetworkScenario::FullyConnected,
            "quorum_loss" if c.number_of_nodes >= 5 => NetworkScenario::QuorumLoss,
            "constrained_election" if c.number_of_nodes >= 5 => {
                NetworkScenario::ConstrainedElection
            }
            "chained" if c.number_of_nodes == 3 => NetworkScenario::Chained,
            "periodic_full" => NetworkScenario::PeriodicFull,
            _ => {
                return Err(BenchmarkError::InvalidTest(format!(
                    "Unimplemented network scenario for {} nodes: {}",
                    c.number_of_nodes, &c.network_scenario
                )));
            }
        };
        self.network_scenario = Some(network_scenario);
        Ok(())
    }

    /// reads hocon config file and returns (timeout, meta_results path, size of preloaded_log)
    pub fn load_benchmark_config<P>(path: P) -> (Duration, Option<String>, u64)
    where
        P: Into<PathBuf>,
    {
        let p: PathBuf = path.into();
        let config = HoconLoader::new()
            .load_file(p)
            .expect("Failed to load file")
            .hocon()
            .expect("Failed to load as HOCON");
        let client_timeout = config["experiment"]["client_timeout"]
            .as_duration()
            .expect("Failed to load client timeout");
        let meta_results_path = config["experiment"]["meta_results_path"].as_string();
        let preloaded_log_size = if cfg!(feature = "preloaded_log") {
            config["experiment"]["preloaded_log_size"]
                .as_i64()
                .expect("Failed to load preloaded_log_size") as u64
        } else {
            0
        };
        (client_timeout, meta_results_path, preloaded_log_size)
    }

    pub fn load_pid_map<P>(path: P, nodes: &[ActorPath]) -> HashMap<ActorPath, u32>
    where
        P: Into<PathBuf>,
    {
        let p: PathBuf = path.into();
        let config = HoconLoader::new()
            .load_file(p)
            .expect("Failed to load file")
            .hocon()
            .expect("Failed to load as HOCON");
        let mut pid_map = HashMap::with_capacity(nodes.len());
        for ap in nodes {
            let addr_str = ap.address().to_string();
            let pid = config["deployment"][addr_str.as_str()]
                .as_i64()
                .unwrap_or_else(|| panic!("Failed to load pid map of {}", addr_str))
                as u32;
            pid_map.insert(ap.clone(), pid);
        }
        pid_map
    }

    fn get_meta_results_dir(&self, results_type: Option<&str>) -> String {
        let meta = self
            .meta_results_path
            .as_ref()
            .expect("No meta results path!");
        let sub_dir = self
            .meta_results_sub_dir
            .as_ref()
            .expect("No meta results sub dir path!");
        format!("{}/{}/{}", meta, sub_dir, results_type.unwrap_or("/"))
    }

    fn persist_timestamp_results(
        &mut self,
        timestamps: Vec<Instant>,
        leader_changes: Vec<(SystemTime, (u64, u64))>,
        reconfig_ts: Option<(SystemTime, SystemTime)>,
    ) {
        let timestamps_dir = self.get_meta_results_dir(Some("timestamps"));
        create_dir_all(&timestamps_dir)
            .unwrap_or_else(|_| panic!("Failed to create given directory: {}", &timestamps_dir));
        let mut timestamps_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!(
                "{}/raw_{}.data",
                &timestamps_dir,
                self.experiment_str.as_ref().unwrap()
            ))
            .expect("Failed to open timestamps file");

        if let Some((reconfig_start, reconfig_end)) = reconfig_ts {
            let start_ts = DateTime::<Utc>::from(reconfig_start);
            let end_ts = DateTime::<Utc>::from(reconfig_end);
            writeln!(timestamps_file, "r,{},{} ", start_ts, end_ts)
                .expect("Failed to write reconfig timestamps to timestamps file");
        }
        for (leader_change_ts, lc) in leader_changes {
            let ts = DateTime::<Utc>::from(leader_change_ts);
            writeln!(timestamps_file, "l,{},{:?} ", ts, lc)
                .expect("Failed to write leader changes to timestamps file");
        }
        for ts in timestamps {
            let timestamp = ts.as_u64();
            writeln!(timestamps_file, "{}", timestamp)
                .expect("Failed to write raw timestamps file");
        }
        writeln!(timestamps_file, "").unwrap();
        timestamps_file
            .flush()
            .expect("Failed to flush raw timestamps file");
    }

    fn persist_windowed_results(&mut self, windowed_res: Vec<usize>) {
        let windowed_dir = self.get_meta_results_dir(Some("windowed"));
        create_dir_all(&windowed_dir)
            .unwrap_or_else(|_| panic!("Failed to create given directory: {}", &windowed_dir));
        let mut windowed_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!(
                "{}/{}.data",
                &windowed_dir,
                self.experiment_str.as_ref().unwrap()
            ))
            .expect("Failed to open windowed file");
        let mut prev_n = 0;
        for n in windowed_res {
            write!(windowed_file, "{},", n - prev_n).expect("Failed to write windowed file");
            prev_n = n;
        }
        writeln!(windowed_file).expect("Failed to write windowed file");
        windowed_file
            .flush()
            .expect("Failed to flush windowed file");
    }

    fn persist_latency_results(&mut self, latencies: &[Duration]) {
        let latency_dir = self.get_meta_results_dir(Some("latency"));
        create_dir_all(&latency_dir)
            .unwrap_or_else(|_| panic!("Failed to create given directory: {}", &latency_dir));
        let mut latency_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!(
                "{}/raw_{}.data",
                &latency_dir,
                self.experiment_str.as_ref().unwrap()
            ))
            .expect("Failed to open latency file");

        let histo = self.latency_hist.as_mut().unwrap();
        for l in latencies {
            let latency = l.as_millis() as u64;
            writeln!(latency_file, "{}", latency).expect("Failed to write raw latency");
            histo.record(latency).expect("Failed to record histogram");
        }
        latency_file
            .flush()
            .expect("Failed to flush raw latency file");
    }

    fn persist_latency_summary(&mut self) {
        let dir = self.get_meta_results_dir(Some("latency"));
        create_dir_all(&dir)
            .unwrap_or_else(|_| panic!("Failed to create given directory: {}", dir));
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!(
                "{}/summary_{}.out",
                &dir,
                self.experiment_str.as_ref().unwrap()
            ))
            .expect("Failed to open latency file");
        let hist = std::mem::take(&mut self.latency_hist).unwrap();
        let quantiles = [
            0.001, 0.01, 0.005, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999,
        ];
        for q in &quantiles {
            writeln!(
                file,
                "Value at quantile {}: {} micro s",
                q,
                hist.value_at_quantile(*q)
            )
            .expect("Failed to write summary latency file");
        }
        let max = hist.max();
        writeln!(
            file,
            "Min: {} micro s, Max: {} micro s, Average: {} micro s",
            hist.min(),
            max,
            hist.mean()
        )
        .expect("Failed to write histogram summary");
        writeln!(file, "Total elements: {}", hist.len())
            .expect("Failed to write histogram summary");
        file.flush().expect("Failed to flush histogram file");
    }

    fn persist_timeouts_retried_summary(&mut self) {
        let timed_out_sum: u64 = self.num_timed_out.iter().sum();
        let retried_sum: u64 = self.num_retried.iter().sum();
        if timed_out_sum > 0 || retried_sum > 0 {
            let meta_path = self
                .meta_results_path
                .as_ref()
                .expect("No meta results path!");
            let summary_file_path = format!("{}/summary.out", meta_path);
            create_dir_all(meta_path).unwrap_or_else(|_| {
                panic!("Failed to create given directory: {}", summary_file_path)
            });
            let mut summary_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(summary_file_path)
                .expect("Failed to open meta summary file");
            writeln!(summary_file, "{}", self.experiment_str.as_ref().unwrap())
                .expect("Failed to write meta summary file");

            if timed_out_sum > 0 {
                let len = self.num_timed_out.len();
                let timed_out_len = self.num_timed_out.iter().filter(|x| **x > 0).count();
                self.num_timed_out.sort();
                let min = self.num_timed_out.first().unwrap();
                let max = self.num_timed_out.last().unwrap();
                let avg = timed_out_sum / (self.iteration_id as u64);
                let median = self.num_timed_out[len / 2];
                let timed_out_summary_str = format!(
                    "{}/{} runs had timeouts. sum: {}, avg: {}, med: {}, min: {}, max: {}",
                    timed_out_len, len, timed_out_sum, avg, median, min, max
                );
                writeln!(summary_file, "{}", timed_out_summary_str).unwrap_or_else(|_| {
                    panic!(
                        "Failed to write meta summary file: {}",
                        timed_out_summary_str
                    )
                });
            }
            if retried_sum > 0 {
                let len = self.num_retried.len();
                let retried_len = self.num_retried.iter().filter(|x| **x > 0).count();
                self.num_retried.sort_unstable();
                let min = self.num_retried.first().unwrap();
                let max = self.num_retried.last().unwrap();
                let avg = retried_sum / (self.iteration_id as u64);
                let median = self.num_timed_out[len / 2];
                let retried_summary_str = format!(
                    "{}/{} runs had retries. sum: {}, avg: {}, med: {}, min: {}, max: {}",
                    retried_len, len, retried_sum, avg, median, min, max
                );
                writeln!(summary_file, "{}", retried_summary_str).unwrap_or_else(|_| {
                    panic!("Failed to write meta summary file: {}", retried_summary_str)
                });
            }
            summary_file.flush().expect("Failed to flush meta file");
        }
    }
}

impl DistributedBenchmarkMaster for AtomicBroadcastMaster {
    type MasterConf = AtomicBroadcastRequest;
    type ClientConf = ClientParams;
    type ClientData = ActorPath;

    fn setup(
        &mut self,
        c: Self::MasterConf,
        m: &DeploymentMetaData,
    ) -> Result<Self::ClientConf, BenchmarkError> {
        println!("Setting up Atomic Broadcast (Master)");
        self.validate_experiment_params(&c, m.number_of_clients())?;
        let experiment_str = create_experiment_str(&c);
        self.num_nodes = Some(c.number_of_nodes);
        self.experiment_str = Some(experiment_str.clone());
        self.num_proposals = Some(c.number_of_proposals.clone());
        self.local_proposal_file = Some(c.file_path.clone());
        self.concurrent_proposals = Some(c.concurrent_proposals);
        self.meta_results_sub_dir = Some(create_metaresults_sub_dir(
            c.number_of_nodes,
            c.concurrent_proposals,
            c.get_reconfiguration(),
        ));
        if c.concurrent_proposals == 1 || cfg!(feature = "track_latency") {
            self.latency_hist =
                Some(Histogram::<u64>::new(4).expect("Failed to create latency histogram"));
        }
        let mut conf = KompactConfig::default();
        conf.load_config_file(CONFIG_PATH);
        let bc = BufferConfig::from_config_file(CONFIG_PATH);
        bc.validate();
        let system = crate::kompact_system_provider::global()
            .new_remote_system_with_threads_config("atomicbroadcast", 4, conf, bc, TCP_NODELAY);
        self.system = Some(system);
        let params = ClientParams::with(experiment_str);
        Ok(params)
    }

    fn prepare_iteration(&mut self, d: Vec<Self::ClientData>) -> () {
        println!("Preparing iteration");
        if self.system.is_none() {
            panic!("No KompactSystem found!")
        }
        let finished_latch = Arc::new(CountdownEvent::new(1));
        self.finished_latch = Some(finished_latch);
        self.iteration_id += 1;
        let num_nodes_needed = self.num_nodes_needed.expect("No cached num_nodes") as usize;
        let mut nodes = d;
        nodes.truncate(num_nodes_needed);
        let (client_timeout, meta_path, preloaded_log_size) =
            Self::load_benchmark_config(CONFIG_PATH);
        let pid_map: Option<HashMap<ActorPath, u32>> = if cfg!(feature = "use_pid_map") {
            Some(Self::load_pid_map(CONFIG_PATH, nodes.as_slice()))
        } else {
            None
        };
        self.meta_results_path = meta_path;
        let mut nodes_id: HashMap<u64, ActorPath> = HashMap::new();
        match &pid_map {
            Some(pm) => {
                for (ap, pid) in pm {
                    nodes_id.insert(*pid as u64, ap.clone());
                }
                nodes.clear();
                for i in 1..=(num_nodes_needed as u64) {
                    nodes.push(nodes_id.get(&i).unwrap().clone());
                }
            }
            None => {
                for (id, ap) in nodes.iter().enumerate() {
                    nodes_id.insert(id as u64 + 1, ap.clone());
                }
            }
        }
        let leader_election_latch = Arc::new(CountdownEvent::new(1));
        let (client_comp, client_path) = self.create_client(
            nodes_id,
            self.network_scenario.expect("No network scenario"),
            client_timeout,
            preloaded_log_size,
            leader_election_latch.clone(),
        );
        let partitioning_actor = self.initialise_iteration(nodes, client_path, pid_map);
        partitioning_actor
            .actor_ref()
            .tell(IterationControlMsg::Run);
        leader_election_latch.wait(); // wait until leader is established
        self.partitioning_actor = Some(partitioning_actor);
        self.client_comp = Some(client_comp);
    }

    fn run_iteration(&mut self) -> () {
        println!("Running Atomic Broadcast experiment!");
        match self.client_comp {
            Some(ref client_comp) => {
                client_comp.actor_ref().tell(LocalClientMessage::Run);
                let finished_latch = self.finished_latch.take().unwrap();
                finished_latch.wait();
            }
            _ => panic!("No client found!"),
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, exec_time_millis: f64) -> () {
        println!(
            "Cleaning up Atomic Broadcast (master) iteration {}. Exec_time: {}",
            self.iteration_id, exec_time_millis
        );
        let system = self.system.take().unwrap();
        let client = self.client_comp.take().unwrap();
        let meta_results: MetaResults = client
            .actor_ref()
            .ask_with(|promise| LocalClientMessage::Stop(Ask::new(promise, ())))
            .wait();
        self.num_timed_out.push(meta_results.num_timed_out);
        self.num_retried.push(meta_results.num_retried);
        self.persist_windowed_results(meta_results.windowed_results);
        if self.concurrent_proposals == Some(1) || cfg!(feature = "track_latency") {
            self.persist_latency_results(&meta_results.latencies);
        }
        if meta_results.reconfig_ts.is_some() || !meta_results.leader_changes.is_empty() {
            self.persist_timestamp_results(
                meta_results.timestamps,
                meta_results.leader_changes,
                meta_results.reconfig_ts,
            );
        }

        let kill_client_f = system.kill_notify(client);
        kill_client_f
            .wait_timeout(REGISTER_TIMEOUT)
            .expect("Client never died");

        if let Some(partitioning_actor) = self.partitioning_actor.take() {
            let kill_pactor_f = system.kill_notify(partitioning_actor);
            kill_pactor_f
                .wait_timeout(REGISTER_TIMEOUT)
                .expect("Partitioning Actor never died!");
        }

        if last_iteration {
            println!("Cleaning up last iteration");
            self.persist_timeouts_retried_summary();
            if self.concurrent_proposals == Some(1) || cfg!(feature = "track_latency") {
                self.persist_latency_summary();
            }
            self.num_nodes = None;
            self.num_nodes_needed = None;
            self.reconfiguration = None;
            self.concurrent_proposals = None;
            self.num_proposals = None;
            self.local_proposal_file = None;
            self.experiment_str = None;
            self.meta_results_sub_dir = None;
            self.num_timed_out.clear();
            self.iteration_id = 0;
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
        } else {
            self.system = Some(system);
        }
    }
}
