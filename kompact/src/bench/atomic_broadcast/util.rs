pub(crate) mod exp_util {
    use benchmark_suite_shared::{
        kompics_benchmarks::benchmarks::AtomicBroadcastRequest, BenchmarkError,
    };
    use hocon::HoconLoader;
    use kompact::prelude::Buf;
    use omnipaxos_core::storage::{memory_storage::MemoryStorage, Entry, Snapshot, Storage};
    use crate::bench::atomic_broadcast::preprocessing::{split_query, merge_query};
    use lecar::controller::Controller;
    use serde::{de::DeserializeOwned, Serialize, Deserialize};
    use std::{fmt::Debug, hash::Hash, path::PathBuf, time::Duration};
    use std::time::Instant;

    pub const TCP_NODELAY: bool = true;
    pub const CONFIG_PATH: &str = "./configs/atomic_broadcast.conf";
    pub const PAXOS_PATH: &str = "paxos_replica";
    pub const RAFT_PATH: &str = "raft_replica";
    pub const REGISTER_TIMEOUT: Duration = Duration::from_secs(5);

    pub const WINDOW_DURATION: Duration = Duration::from_millis(5000);
    pub const DATA_SIZE: usize = 8;

    pub trait LogCommand:
        Entry + Serialize + DeserializeOwned + Send + Sync + 'static + Debug
    {
        type Response: Serialize + DeserializeOwned + Send + Debug + Clone + Eq + Hash;

        fn with(id: u64, sql: String) -> Self;
        fn create_response(&self) -> Self::Response;
    }
    pub trait LogSnapshot<T: LogCommand>: Snapshot<T> + Send + Sync + 'static + Debug {}
    pub trait ReplicaStore<T: LogCommand, S: LogSnapshot<T>>:
        Storage<T, S> + Default + Send + Sync + 'static
    {
    }

    // pub type EntryType = Vec<u8>;
    // impl LogCommand for EntryType {
    //     type Response = u64;

    //     fn with(id: u64, _sql: String) -> Self {
    //         bincode::serialize(&id).expect("Failed to serialize data id")
    //     }

    //     fn create_response(&self) -> Self::Response {
    //         bincode::deserialize_from(self.as_slice().reader())
    //             .expect("Failed to deserialize data id")
    //     }
    // }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct StoreCommand {
        pub id: u64,
        pub sql: String,
    }
    impl LogCommand for StoreCommand {
        type Response = u64;

        fn with(id: u64, sql: String) -> Self {
            StoreCommand { id, sql}
        }

        fn create_response(&self) -> Self::Response {
            self.id
        }
    }

    impl Entry for StoreCommand {

        #[cfg(feature = "enable_cache_compression")]
        fn encode(&mut self, cache: &mut Controller) {
            let (template, parameters) = split_query(&self.sql);

            let now = Instant::now();
            #[cfg(feature = "track_cache_overhead")]
            {
                // let now = Instant::now();
                let len_tuple = cache.len();
                cache.counter.size = (len_tuple.0 + len_tuple.1 + len_tuple.2) as u64;
                cache.counter.num_queries += 1;
                cache.counter.raw_messsages_size += self.sql.len() as u64;
            }
            
            let mut hit = false;
            if let Some(index) = cache.get_index_of(&template) {
                // exists in cache
                // send index and parameters
                let compressed = format!("1*|*{}*|*{}", index.to_string(), parameters);

                self.sql = compressed;
                hit = true
            } else {
                // send template and parameters
                let uncompressed = format!("0*|*{}*|*{}", template, parameters);

                self.sql = uncompressed;
            }

            // update cache for leader
            cache.insert(&template, 0);
            //println!("Encode query {}:{}", self.id, self.sql);

            #[cfg(feature = "track_cache_overhead")] 
            {
                if hit {
                        cache.counter.hits += 1;
                } else {
                    cache.counter.misses += 1;
                }
            
                let elapsed = now.elapsed();
                cache.counter.compressed_size += self.sql.len() as u64;
                cache.counter.compression_time += elapsed.as_micros() as u64;
                cache.counter.memory_size = cache.print_size() as u64;
                cache.counter.try_write_to_file("counter_logs.txt");
            }
        }

        #[cfg(feature = "cache_compression2")]
        fn encode(&mut self, cache: &mut Controller) {
             let now = Instant::now();
             let parameters = &self.sql.split(" ").collect::<Vec<&str>>();
             
             #[cfg(feature = "track_cache_overhead")] 
             {     
                let len_tuple = cache.len();
                cache.counter.size = (len_tuple.0 + len_tuple.1 + len_tuple.2) as u64;
                cache.counter.num_queries += 1;
                cache.counter.raw_messsages_size += self.sql.len() as u64;
             }

             let compressed = parameters.iter()
                 .map(|p| {
                     if let Some(index) = cache.get_index_of(p) {
                         let index_str = "_".to_owned() + &index.to_string();
                         index_str
                     } else {
                         (**p).to_string()
                     }
                 })
                 .collect::<Vec<String>>()
                 .join(",");
      
             #[cfg(feature = "track_cache_overhead")]
             {
                 let elapsed = now.elapsed();
                 cache.counter.compressed_size += compressed.len() as u64;
                 cache.counter.compression_time += elapsed.as_micros() as u64;
                 cache.counter.try_write_to_file("counter_logs.txt");
             }

             // update cache for leader
             for para in parameters {
                 cache.insert(&para, para.to_string());
             }
            
             self.sql = compressed;
         }

        #[cfg(feature = "enable_cache_compression")]
        fn decode(&mut self, cache: &mut Controller) {
            //println!("Decode query {}:{}", self.id, self.sql);

            let parts: Vec<&str> = self.sql.split("*|*").collect();
            if parts.len() != 3 { 
                panic!("Unexpected query: {:?}", self.sql);
            }

            let (compressed, index_or_template, parameters) = (parts[0], parts[1].to_string(), parts[2].to_string());
            let mut template = index_or_template.clone();

            if compressed == "1" {
                // compressed messsage
                let index = index_or_template.parse::<usize>().unwrap();
                if let Some(cacheitem) = cache.get_index(index) {
                    template = cacheitem.to_string();
                } else { 
                    let index = index;
                    let id = self.id;
                    let sql = self.sql.clone();
                    let size = cache.len();

                    panic!("Query {}:{} is out of index: {}/{:?}", id, sql, index, size);
                }
            }
            
            // update cache for followers
            cache.insert(&template, 0);
            self.sql = merge_query(template, parameters);
        }

        #[cfg(feature = "cache_compression2")]
        fn decode(&mut self, cache: &mut Controller) {
            let parts: Vec<&str> = self.sql.split(",").collect();

            let uncompressed = parts.iter()
                .map(|p| {
                    if (**p).starts_with("_") {
                        let index = (**p)[1..].parse::<usize>().unwrap();
                        cache.get_index(index).unwrap().value().to_string()
                    } else {
                        (**p).to_string()
                    }
                })
                .collect::<Vec<String>>();
         
            self.sql = uncompressed.join(" ");
            // update cache for followers
            for para in uncompressed {
                cache.insert(&para, para.clone());
            }
        }
    }

    pub type SnapshotType = ();
    pub type PaxosStorageType = MemoryStorage<StoreCommand, SnapshotType>;

    // impl LogSnapshot<EntryType> for SnapshotType {}
    impl ReplicaStore<StoreCommand, SnapshotType> for PaxosStorageType {}
    impl LogSnapshot<StoreCommand> for () {}

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
            c.number_of_proposals,
            c.concurrent_proposals,
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
