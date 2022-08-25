pub(crate) mod exp_util {
    use benchmark_suite_shared::{
        kompics_benchmarks::benchmarks::AtomicBroadcastRequest, BenchmarkError,
    };
    use hocon::HoconLoader;
    use std::{path::PathBuf, time::Duration};

    pub const TCP_NODELAY: bool = true;
    pub const CONFIG_PATH: &str = "./configs/atomic_broadcast.conf";
    pub const PAXOS_PATH: &str = "paxos_replica";
    pub const RAFT_PATH: &str = "raft_replica";
    pub const REGISTER_TIMEOUT: Duration = Duration::from_secs(5);

    pub const WINDOW_DURATION: Duration = Duration::from_millis(5000);
    pub const DATA_SIZE: usize = 8;
    pub const WARMUP_DURATION: Duration = Duration::from_secs(20);
    pub const COOLDOWN_DURATION: Duration = WARMUP_DURATION;

    pub struct ExperimentParams {
        pub election_timeout: u64,
        pub outgoing_period: Duration,
        pub max_inflight: usize,
        pub initial_election_factor: u64,
        pub preloaded_log_size: u64,
        #[allow(dead_code)]
        io_meta_results_path: String,
        #[allow(dead_code)]
        experiment_str: String,
    }

    impl ExperimentParams {
        pub fn new(
            election_timeout: u64,
            outgoing_period: Duration,
            max_inflight: usize,
            initial_election_factor: u64,
            preloaded_log_size: u64,
            io_meta_results_path: String,
            experiment_str: String,
        ) -> ExperimentParams {
            ExperimentParams {
                election_timeout,
                outgoing_period,
                max_inflight,
                initial_election_factor,
                preloaded_log_size,
                io_meta_results_path,
                experiment_str,
            }
        }

        pub fn load_from_file(
            path: &str,
            meta_sub_dir: String,
            experiment_str: String,
        ) -> ExperimentParams {
            let p: PathBuf = path.into();
            let config = HoconLoader::new()
                .load_file(p)
                .expect("Failed to load file")
                .hocon()
                .expect("Failed to load as HOCON");
            let election_timeout = config["experiment"]["election_timeout"]
                .as_i64()
                .expect("Failed to load election_timeout")
                as u64;
            let outgoing_period = config["experiment"]["outgoing_period"]
                .as_duration()
                .expect("Failed to load outgoing_period");
            let max_inflight = config["experiment"]["max_inflight"]
                .as_i64()
                .expect("Failed to load max_inflight") as usize;
            let initial_election_factor = config["experiment"]["initial_election_factor"]
                .as_i64()
                .expect("Failed to load initial_election_factor")
                as u64;
            let preloaded_log_size = config["experiment"]["preloaded_log_size"]
                .as_i64()
                .expect("Failed to load preloaded_log_size")
                as u64;
            let meta_path = config["experiment"]["meta_results_path"]
                .as_string()
                .expect("No path for meta results!");
            let io_meta_results_path = format!("{}/{}/io", meta_path, meta_sub_dir); // meta_results/3-10k/io/paxos,3,10000.data
            ExperimentParams::new(
                election_timeout,
                outgoing_period,
                max_inflight,
                initial_election_factor,
                preloaded_log_size,
                io_meta_results_path,
                experiment_str,
            )
        }

        #[cfg(feature = "measure_io")]
        pub fn get_io_meta_results_file(&self) -> File {
            create_dir_all(&self.io_meta_results_path)
                .expect("Failed to create io meta directory: {}");
            let io_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!(
                    "{}/{}.data",
                    self.io_meta_results_path.as_str(),
                    self.experiment_str.as_str()
                ))
                .expect("Failed to open timestamps file");
            io_file
        }
    }

    pub fn create_experiment_str(c: &AtomicBroadcastRequest) -> String {
        format!(
            "{},{},{},{},{},{},{}",
            c.algorithm,
            c.number_of_nodes,
            c.concurrent_proposals,
            format!("{}min", c.duration_secs / 60), // TODO
            c.reconfiguration.clone(),
            c.reconfig_policy,
            c.network_scenario,
        )
    }

    pub fn create_metaresults_sub_dir(
        number_of_nodes: u64,
        concurrent_proposals: u64,
        reconfiguration: &str,
    ) -> String {
        format!(
            "{}-{}-{}",
            number_of_nodes, concurrent_proposals, reconfiguration
        )
    }

    pub fn get_reconfig_nodes(s: &str, n: u64) -> Result<Option<Vec<u64>>, BenchmarkError> {
        match s.to_lowercase().as_ref() {
            "off" => Ok(None),
            "single" => Ok(Some(vec![n + 1])),
            "majority" => {
                let majority = n / 2 + 1;
                let new_nodes: Vec<u64> = (n + 1..n + 1 + majority).collect();
                Ok(Some(new_nodes))
            }
            _ => Err(BenchmarkError::InvalidMessage(String::from(
                "Got unknown reconfiguration parameter",
            ))),
        }
    }
}

#[cfg(feature = "measure_io")]
pub mod io_metadata {
    use pretty_bytes::converter::convert;
    use std::{fmt, ops::Add};

    #[derive(Copy, Clone, Default, Eq, PartialEq)]
    pub struct IOMetaData {
        msgs_sent: usize,
        bytes_sent: usize,
        msgs_received: usize,
        bytes_received: usize,
    }

    impl IOMetaData {
        pub fn update_received<T>(&mut self, msg: &T) {
            let size = std::mem::size_of_val(msg);
            self.bytes_received += size;
            self.msgs_received += 1;
        }

        pub fn update_sent<T>(&mut self, msg: &T) {
            let size = std::mem::size_of_val(msg);
            self.bytes_sent += size;
            self.msgs_sent += 1;
        }

        pub fn update_sent_with_size(&mut self, size: usize) {
            self.bytes_sent += size;
            self.msgs_sent += 1;
        }

        pub fn update_received_with_size(&mut self, size: usize) {
            self.bytes_received += size;
            self.msgs_received += 1;
        }

        pub fn reset(&mut self) {
            self.msgs_received = 0;
            self.bytes_received = 0;
            self.msgs_sent = 0;
            self.bytes_sent = 0;
        }
    }

    impl fmt::Debug for IOMetaData {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_fmt(format_args!(
                "Sent: ({}, {:?}), Received: ({}, {:?})",
                self.msgs_sent,
                &convert(self.bytes_sent as f64),
                self.msgs_received,
                &convert(self.bytes_received as f64)
            ))
        }
    }

    impl Add for IOMetaData {
        type Output = Self;

        fn add(self, other: Self) -> Self {
            Self {
                msgs_received: self.msgs_received + other.msgs_received,
                bytes_received: self.bytes_received + other.bytes_received,
                msgs_sent: self.msgs_sent + other.msgs_sent,
                bytes_sent: self.bytes_sent + other.bytes_sent,
            }
        }
    }
}
