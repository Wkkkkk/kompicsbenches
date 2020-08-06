pub const ELECTION_TIMEOUT: u64 = 10000;
pub const OUTGOING_MSGS_PERIOD: u64 = 1;
pub const MAX_INFLIGHT: usize = 10000000;   // capacity of number of messages in parallel. Set to max batch size in experiment test space
pub const INITIAL_ELECTION_FACTOR: u64 = 10;   // shorter first election: ELECTION_TIMEOUT/INITIAL_ELECTION_FACTOR
pub const META_RESULTS_DIR: &str = "../meta_results/meta-distributed-aws"; // change for each benchmark

pub mod paxos {
    pub const GET_DECIDED_PERIOD: u64 = 1;
    pub const TRANSFER_TIMEOUT: u64 = 5000;
    pub const BLE_DELTA: u64 = 100;
    pub const BATCH_DECIDE: bool = true;
    pub const BLE_PRIO_START: bool = true;
    pub const MAX_ACCSYNC: bool = true;
}

pub mod raft {
    pub const TICK_PERIOD: u64 = 100;   // one tick = +1 in logical clock
    pub const LEADER_HEARTBEAT_PERIOD: u64 = 1000;
    pub const MAX_BATCH_SIZE: u64 = u64::max_value();
}

pub mod client {
    pub const PROPOSAL_TIMEOUT: u64 = 50000;
}

pub mod max_accsync_experiments {
    #[derive(Debug, Eq, PartialEq)]
    pub enum Mode {
        All,
        Majority,
        Leader,
    }
    pub const EXPERIMENT_MODE: Mode = Mode::Majority;
    pub const NUM_ELEMENTS: usize = 20000000;
    pub const NUM_PROPOSALS: usize = 1000000;
    pub const SLOW_LD: u64 = (NUM_ELEMENTS/2) as u64;
}
