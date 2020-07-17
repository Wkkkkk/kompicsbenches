extern crate raft as tikv_raft;

use kompact::prelude::*;
use crate::serialiser_ids;
use protobuf::{Message, parse_from_bytes};

pub mod raft {
    extern crate raft as tikv_raft;
    use tikv_raft::prelude::Message as TikvRaftMsg;
    use kompact::prelude::{SerError, BufMut, Deserialiser, Buf};
    use super::*;

    pub struct RawRaftSer;

    #[derive(Debug)]
    pub struct RaftMsg(pub TikvRaftMsg);    // wrapper to implement eager serialisation

    impl Serialisable for RaftMsg {
        fn ser_id(&self) -> u64 {
            serialiser_ids::RAFT_ID
        }

        fn size_hint(&self) -> Option<usize> {
            Some(500)   // TODO
        }

        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            let bytes = self.0.write_to_bytes().expect("Protobuf failed to serialise TikvRaftMsg");
            buf.put_slice(&bytes);
            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }

    impl Deserialiser<TikvRaftMsg> for RawRaftSer {
        const SER_ID: u64 = serialiser_ids::RAFT_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<TikvRaftMsg, SerError> {
            let bytes = buf.bytes();
            let rm: TikvRaftMsg = parse_from_bytes::<TikvRaftMsg>(bytes).expect("Protobuf failed to deserialise TikvRaftMsg");
            Ok(rm)
        }
    }
}

pub mod paxos {
    use ballot_leader_election::Ballot;
    use super::super::paxos::raw_paxos::Entry;
    use std::fmt::Debug;
    use kompact::prelude::{SerError, BufMut, Deserialiser, Buf, Serialisable, Any};
    use crate::serialiser_ids;
    use crate::bench::atomic_broadcast::paxos::raw_paxos::StopSign;

    #[derive(Clone, Debug)]
    pub struct Prepare {
        pub n: Ballot,
        pub ld: u64,
        pub n_accepted: Ballot,
    }

    impl Prepare {
        pub fn with(n: Ballot, ld: u64, n_accepted: Ballot) -> Prepare {
            Prepare{ n, ld, n_accepted }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Promise {
        pub n: Ballot,
        pub n_accepted: Ballot,
        pub sfx: Vec<Entry>,
        pub ld: u64,
    }

    impl Promise {
        pub fn with(n: Ballot, n_accepted: Ballot, sfx: Vec<Entry>, ld: u64) -> Promise {
            Promise { n, n_accepted, sfx, ld, }
        }
    }

    #[derive(Clone, Debug)]
    pub struct AcceptSync {
        pub n: Ballot,
        pub entries: Vec<Entry>,
        pub ld: u64,
        pub sync: bool, // true -> append on prefix(ld), false -> append
    }

    impl AcceptSync {
        pub fn with(n: Ballot, sfx: Vec<Entry>, ld: u64, sync: bool) -> AcceptSync {
            AcceptSync { n, entries: sfx, ld, sync }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Accept {
        pub n: Ballot,
        pub entry: Entry,
    }

    impl Accept {
        pub fn with(n: Ballot, entry: Entry) -> Accept {
            Accept{ n, entry }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Accepted {
        pub n: Ballot,
        pub la: u64,
    }

    impl Accepted {
        pub fn with(n: Ballot, la: u64) -> Accepted {
            Accepted{ n, la }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Decide {
        pub ld: u64,
        pub n: Ballot,
    }

    impl Decide {
        pub fn with(ld: u64, n: Ballot) -> Decide {
            Decide{ ld, n }
        }
    }

    #[derive(Clone, Debug)]
    pub enum PaxosMsg {
        Prepare(Prepare),
        Promise(Promise),
        AcceptSync(AcceptSync),
        Accept(Accept),
        Accepted(Accepted),
        Decide(Decide),
        ProposalForward(Entry)
    }

    #[derive(Clone, Debug)]
    pub struct Message {
        pub from: u64,
        pub to: u64,
        pub msg: PaxosMsg
    }

    impl Message {
        pub fn with(from: u64, to: u64, msg: PaxosMsg) -> Message {
            Message{ from, to, msg }
        }
    }

    const PREPARE_ID: u8 = 1;
    const PROMISE_ID: u8 = 2;
    const ACCEPTSYNC_ID: u8 = 3;
    const ACCEPT_ID: u8 = 4;
    const ACCEPTED_ID: u8 = 5;
    const DECIDE_ID: u8 = 6;
    const PROPOSALFORWARD_ID: u8 = 7;

    const NORMAL_ENTRY_ID: u8 = 1;
    const SS_ENTRY_ID: u8 = 2;

    // const PAXOS_MSG_OVERHEAD: usize = 17;
    // const BALLOT_OVERHEAD: usize = 16;
    // const DATA_SIZE: usize = 8;
    // const ENTRY_OVERHEAD: usize = 21 + DATA_SIZE;

    pub struct PaxosSer;

    impl PaxosSer {
        fn serialise_ballot(ballot: &Ballot, buf: &mut dyn BufMut) {
            buf.put_u64(ballot.n);
            buf.put_u64(ballot.pid);
        }

        fn serialise_entry(e: &Entry, buf: &mut dyn BufMut) {
            match e {
                Entry::Normal(data) => {
                    buf.put_u8(NORMAL_ENTRY_ID);
                    buf.put_u32(data.len() as u32);
                    buf.put_slice(data.as_slice());
                }
                Entry::StopSign(ss) => {
                    buf.put_u8(SS_ENTRY_ID);
                    buf.put_u32(ss.config_id);
                    buf.put_u32(ss.nodes.len() as u32);
                    ss.nodes.iter().for_each(|pid| buf.put_u64(*pid));
                }
            }
        }

        pub(crate) fn serialise_entries(ents: &[Entry], buf: &mut dyn BufMut) {
            buf.put_u32(ents.len() as u32);
            for e in ents {
                Self::serialise_entry(e, buf);
            }
        }

        fn deserialise_ballot(buf: &mut dyn Buf) -> Ballot {
            let n = buf.get_u64();
            let pid = buf.get_u64();
            Ballot::with(n, pid)
        }

        fn deserialise_entry(buf: &mut dyn Buf) -> Entry {
            match buf.get_u8() {
                NORMAL_ENTRY_ID => {
                    let data_len = buf.get_u32() as usize;
                    let mut data = vec![0; data_len];
                    buf.copy_to_slice(&mut data);
                    Entry::Normal(data)
                },
                SS_ENTRY_ID => {
                    let config_id = buf.get_u32();
                    let nodes_len = buf.get_u32() as usize;
                    let mut nodes = Vec::with_capacity(nodes_len);
                    for _ in 0..nodes_len {
                        nodes.push(buf.get_u64());
                    }
                    let ss = StopSign::with(config_id, nodes);
                    Entry::StopSign(ss)
                },
                error_id => panic!(format!("Got unexpected id in deserialise_entry: {}", error_id)),
            }
        }

        pub fn deserialise_entries(buf: &mut dyn Buf) -> Vec<Entry> {
            let len = buf.get_u32();
            let mut ents = Vec::with_capacity(len as usize);
            for _ in 0..len {
                ents.push(Self::deserialise_entry(buf));
            }
            ents
        }
    }

    impl Serialisable for Message {
        fn ser_id(&self) -> u64 {
            serialiser_ids::PAXOS_ID
        }

        fn size_hint(&self) -> Option<usize> {
            Some(500)
            /*match &self.msg {
                PaxosMsg::Prepare(_) => Some(PAXOS_MSG_OVERHEAD + 2 * BALLOT_OVERHEAD + 8),
                PaxosMsg::Promise(p) => Some(PAXOS_MSG_OVERHEAD + 2 * BALLOT_OVERHEAD + 8 + 4 + p.sfx.len() * ENTRY_OVERHEAD),
                PaxosMsg::AcceptSync(acc_sync) => Some(PAXOS_MSG_OVERHEAD + BALLOT_OVERHEAD + 4 + acc_sync.sfx.len() * ENTRY_OVERHEAD + 8),
                PaxosMsg::Accept(_) => Some(PAXOS_MSG_OVERHEAD + BALLOT_OVERHEAD + 4 + ENTRY_OVERHEAD),
                PaxosMsg::Accepted(_) => Some(PAXOS_MSG_OVERHEAD + BALLOT_OVERHEAD + 8),
                PaxosMsg::Decide(_) => Some(PAXOS_MSG_OVERHEAD + BALLOT_OVERHEAD + 8),
                PaxosMsg::ProposalForward(pf) => Some(PAXOS_MSG_OVERHEAD + 4 + pf.len() * ENTRY_OVERHEAD),
            }*/
        }

        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            buf.put_u64(self.from);
            buf.put_u64(self.to);
            match &self.msg {
                PaxosMsg::Prepare(p) => {
                    buf.put_u8(PREPARE_ID);
                    PaxosSer::serialise_ballot(&p.n, buf);
                    PaxosSer::serialise_ballot(&p.n_accepted, buf);
                    buf.put_u64(p.ld);
                },
                PaxosMsg::Promise(p) => {
                    buf.put_u8(PROMISE_ID);
                    PaxosSer::serialise_ballot(&p.n, buf);
                    PaxosSer::serialise_ballot(&p.n_accepted, buf);
                    buf.put_u64(p.ld);
                    PaxosSer::serialise_entries(&p.sfx, buf);
                },
                PaxosMsg::AcceptSync(acc_sync) => {
                    buf.put_u8(ACCEPTSYNC_ID);
                    buf.put_u64(acc_sync.ld);
                    let sync: u8 = if acc_sync.sync { 1 } else { 0 };
                    buf.put_u8(sync);
                    PaxosSer::serialise_ballot(&acc_sync.n, buf);
                    PaxosSer::serialise_entries(&acc_sync.entries, buf);
                },
                PaxosMsg::Accept(a) => {
                    buf.put_u8(ACCEPT_ID);
                    PaxosSer::serialise_ballot(&a.n, buf);
                    PaxosSer::serialise_entry(&a.entry, buf);
                },
                PaxosMsg::Accepted(acc) => {
                    buf.put_u8(ACCEPTED_ID);
                    PaxosSer::serialise_ballot(&acc.n, buf);
                    buf.put_u64(acc.la);
                },
                PaxosMsg::Decide(d) => {
                    buf.put_u8(DECIDE_ID);
                    PaxosSer::serialise_ballot(&d.n, buf);
                    buf.put_u64(d.ld);
                },
                PaxosMsg::ProposalForward(entry) => {
                    buf.put_u8(PROPOSALFORWARD_ID);
                    PaxosSer::serialise_entry(entry, buf);
                }
            }
            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }
    impl Deserialiser<Message> for PaxosSer {
        const SER_ID: u64 = serialiser_ids::PAXOS_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<Message, SerError> {
            let from = buf.get_u64();
            let to = buf.get_u64();
            match buf.get_u8() {
                PREPARE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let n_accepted = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let p = Prepare::with(n, ld, n_accepted);
                    let msg = Message::with(from, to, PaxosMsg::Prepare(p));
                    Ok(msg)
                },
                PROMISE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let n_accepted = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let sfx = Self::deserialise_entries(buf);
                    let prom = Promise::with(n, n_accepted, sfx, ld);
                    let msg = Message::with(from, to, PaxosMsg::Promise(prom));
                    Ok(msg)
                },
                ACCEPTSYNC_ID => {
                    let ld = buf.get_u64();
                    let sync = match buf.get_u8() {
                        1 => true,
                        0 => false,
                        _ => panic!("Unexpected sync flag when deserialising acceptsync"),
                    };
                    let n = Self::deserialise_ballot(buf);
                    let sfx = Self::deserialise_entries(buf);
                    let acc_sync = AcceptSync::with(n, sfx, ld, sync);
                    let msg = Message::with(from, to, PaxosMsg::AcceptSync(acc_sync));
                    Ok(msg)
                },
                ACCEPT_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let entry = Self::deserialise_entry(buf);
                    let a = Accept::with(n, entry);
                    let msg = Message::with(from, to, PaxosMsg::Accept(a));
                    Ok(msg)
                },
                ACCEPTED_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let acc = Accepted::with(n, ld);
                    let msg = Message::with(from, to, PaxosMsg::Accepted(acc));
                    Ok(msg)
                },
                DECIDE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let d = Decide::with(ld, n);
                    let msg = Message::with(from, to, PaxosMsg::Decide(d));
                    Ok(msg)
                },
                PROPOSALFORWARD_ID => {
                    let entry = Self::deserialise_entry(buf);
                    let pf = PaxosMsg::ProposalForward(entry);
                    let msg = Message::with(from, to, pf);
                    Ok(msg)
                },
                _ => {
                    Err(SerError::InvalidType(
                        "Found unkown id but expected PaxosMsg".into(),
                    ))
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceMetaData {
        pub config_id: u32,
        pub len: u64
    }

    impl SequenceMetaData {
        pub fn with(config_id: u32, len: u64) -> SequenceMetaData {
            SequenceMetaData{ config_id, len }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceTransfer {
        pub config_id: u32,
        pub tag: u32,
        pub succeeded: bool,
        pub from_idx: u64,
        pub to_idx: u64,
        pub entries: Vec<Entry>,
        pub metadata: SequenceMetaData
    }

    impl SequenceTransfer {
        pub fn with(
            config_id: u32,
            tag: u32,
            succeeded: bool,
            from_idx: u64,
            to_idx: u64,
            entries: Vec<Entry>,
            metadata: SequenceMetaData
        ) -> SequenceTransfer {
            SequenceTransfer { config_id, tag, succeeded, from_idx, to_idx, entries, metadata }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    pub struct SequenceRequest {
        pub config_id: u32,
        pub tag: u32,   // keep track of which segment of the sequence this is
        pub from_idx: u64,
        pub to_idx: u64,
        pub requestor_pid: u64,
    }

    impl SequenceRequest {
        pub fn with(config_id: u32, tag: u32, from_idx: u64, to_idx: u64, requestor_pid: u64) -> SequenceRequest {
            SequenceRequest{ config_id, tag, from_idx, to_idx, requestor_pid }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Reconfig {
        pub continued_nodes: Vec<u64>,
        pub new_nodes: Vec<u64>,
    }

    impl Reconfig {
        pub fn with(continued_nodes: Vec<u64>, new_nodes: Vec<u64>) -> Reconfig {
            Reconfig { continued_nodes, new_nodes }
        }
    }

    #[derive(Clone, Debug)]
    pub enum ReconfigurationMsg {
        Init(ReconfigInit),
        SequenceRequest(SequenceRequest),
        SequenceTransfer(SequenceTransfer),
    }

    #[derive(Clone, Debug)]
    pub struct ReconfigInit {
        pub config_id: u32,
        pub nodes: Reconfig,
        pub seq_metadata: SequenceMetaData,
        pub from: u64
    }

    impl ReconfigInit {
        pub fn with(config_id: u32, nodes: Reconfig, seq_metadata: SequenceMetaData, from: u64) -> ReconfigInit {
            ReconfigInit{ config_id, nodes, seq_metadata, from }
        }
    }

    const RECONFIG_INIT_ID: u8 = 1;
    const SEQ_REQ_ID: u8 = 2;
    const SEQ_TRANSFER_ID: u8 = 3;

    pub struct ReconfigSer;

    impl Serialisable for ReconfigurationMsg {
        fn ser_id(&self) -> u64 {
            serialiser_ids::RECONFIG_ID
        }

        fn size_hint(&self) -> Option<usize> {
            match self {
                ReconfigurationMsg::Init(_) => Some(64),
                ReconfigurationMsg::SequenceRequest(_) => Some(25),
                ReconfigurationMsg::SequenceTransfer(_) => Some(1000),
            }
        }

        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            match self {
                ReconfigurationMsg::Init(r) => {
                    buf.put_u8(RECONFIG_INIT_ID);
                    buf.put_u32(r.config_id);
                    buf.put_u64(r.from);
                    buf.put_u32(r.seq_metadata.config_id);
                    buf.put_u64(r.seq_metadata.len);
                    buf.put_u32(r.nodes.continued_nodes.len() as u32);
                    r.nodes.continued_nodes.iter().for_each(|pid| buf.put_u64(*pid));
                    buf.put_u32(r.nodes.new_nodes.len() as u32);
                    r.nodes.new_nodes.iter().for_each(|pid| buf.put_u64(*pid));
                },
                ReconfigurationMsg::SequenceRequest(sr) => {
                    buf.put_u8(SEQ_REQ_ID);
                    buf.put_u32(sr.config_id);
                    buf.put_u32(sr.tag);
                    buf.put_u64(sr.from_idx);
                    buf.put_u64(sr.to_idx);
                    buf.put_u64(sr.requestor_pid);
                },
                ReconfigurationMsg::SequenceTransfer(st) => {
                    buf.put_u8(SEQ_TRANSFER_ID);
                    buf.put_u32(st.config_id);
                    buf.put_u32(st.tag);
                    let succeeded: u8 = if st.succeeded { 1 } else { 0 };
                    buf.put_u8(succeeded);
                    buf.put_u64(st.from_idx);
                    buf.put_u64(st.to_idx);
                    buf.put_u32(st.metadata.config_id);
                    buf.put_u64(st.metadata.len);
                    PaxosSer::serialise_entries(st.entries.as_slice(), buf);
                }
            }
            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }
    impl Deserialiser<ReconfigurationMsg> for ReconfigSer {
        const SER_ID: u64 = serialiser_ids::RECONFIG_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<ReconfigurationMsg, SerError> {
            match buf.get_u8() {
                RECONFIG_INIT_ID => {
                    let config_id = buf.get_u32();
                    let from = buf.get_u64();
                    let seq_metadata_config_id = buf.get_u32();
                    let seq_metadata_len = buf.get_u64();
                    let continued_nodes_len = buf.get_u32();
                    let mut continued_nodes = Vec::with_capacity(continued_nodes_len as usize);
                    for _ in 0..continued_nodes_len {
                        continued_nodes.push(buf.get_u64());
                    }
                    let new_nodes_len = buf.get_u32();
                    let mut new_nodes = Vec::with_capacity(new_nodes_len as usize);
                    for _ in 0..new_nodes_len {
                        new_nodes.push(buf.get_u64());
                    }
                    let seq_metadata = SequenceMetaData::with(seq_metadata_config_id, seq_metadata_len);
                    let nodes = Reconfig::with(continued_nodes, new_nodes);
                    let r = ReconfigInit::with(config_id, nodes, seq_metadata, from);
                    Ok(ReconfigurationMsg::Init(r))
                },
                SEQ_REQ_ID => {
                    let config_id = buf.get_u32();
                    let tag = buf.get_u32();
                    let from_idx = buf.get_u64();
                    let to_idx = buf.get_u64();
                    let requestor_pid = buf.get_u64();
                    let sr = SequenceRequest::with(config_id, tag, from_idx, to_idx, requestor_pid);
                    Ok(ReconfigurationMsg::SequenceRequest(sr))
                },
                SEQ_TRANSFER_ID => {
                    let config_id = buf.get_u32();
                    let tag = buf.get_u32();
                    let succeeded = buf.get_u8() == 1;
                    let from_idx = buf.get_u64();
                    let to_idx = buf.get_u64();
                    let metadata_config_id = buf.get_u32();
                    let metadata_seq_len = buf.get_u64();
                    let entries = PaxosSer::deserialise_entries(buf);
                    let metadata = SequenceMetaData::with(metadata_config_id, metadata_seq_len);
                    let st = SequenceTransfer::with(config_id, tag, succeeded, from_idx, to_idx, entries, metadata);
                    Ok(ReconfigurationMsg::SequenceTransfer(st))
                }
                _ => {
                    Err(SerError::InvalidType(
                        "Found unkown id but expected ReconfigurationMsg".into(),
                    ))
                }
            }


        }
    }

    pub mod ballot_leader_election {
        use super::super::*;

        #[derive(Clone, Copy, Eq, Debug, Ord, PartialOrd, PartialEq)]
        pub struct Ballot {
            pub n: u64,
            pub pid: u64
        }

        impl Ballot {
            pub fn with(n: u64, pid: u64) -> Ballot {
                Ballot{n, pid}
            }
        }

        #[derive(Copy, Clone, Debug)]
        pub struct Leader {
            pub pid: u64,
            pub ballot: Ballot
        }

        impl Leader {
            pub fn with(pid: u64, ballot: Ballot) -> Leader {
                Leader{pid, ballot}
            }
        }

        #[derive(Clone, Debug)]
        pub enum HeartbeatMsg {
            Request(HeartbeatRequest),
            Reply(HeartbeatReply)
        }

        #[derive(Clone, Debug)]
        pub struct HeartbeatRequest {
            pub round: u64,
            pub max_ballot: Ballot,
        }

        impl HeartbeatRequest {
            pub fn with(round: u64, max_ballot: Ballot) -> HeartbeatRequest {
                HeartbeatRequest {round, max_ballot}
            }
        }

        #[derive(Clone, Debug)]
        pub struct HeartbeatReply {
            pub sender_pid: u64,
            pub round: u64,
            pub max_ballot: Ballot
        }

        impl HeartbeatReply {
            pub fn with(sender_pid: u64, round: u64, max_ballot: Ballot) -> HeartbeatReply {
                HeartbeatReply { sender_pid, round, max_ballot}
            }
        }

        pub struct BallotLeaderSer;

        const HB_REQ_ID: u8 = 1;
        const HB_REP_ID: u8 = 2;

        impl Serialisable for HeartbeatMsg {
            fn ser_id(&self) -> u64 {
                serialiser_ids::BLE_ID
            }

            fn size_hint(&self) -> Option<usize> {
                Some(50)
            }

            fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
                match self {
                    HeartbeatMsg::Request(req) => {
                        buf.put_u8(HB_REQ_ID);
                        buf.put_u64(req.round);
                        buf.put_u64(req.max_ballot.n);
                        buf.put_u64(req.max_ballot.pid);
                    },
                    HeartbeatMsg::Reply(rep) => {
                        buf.put_u8(HB_REP_ID);
                        buf.put_u64(rep.sender_pid);
                        buf.put_u64(rep.round);
                        buf.put_u64(rep.max_ballot.n);
                        buf.put_u64(rep.max_ballot.pid);
                    }
                }
                Ok(())
            }

            fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
                Ok(self)
            }
        }

        impl Deserialiser<HeartbeatMsg> for BallotLeaderSer {
            const SER_ID: u64 = serialiser_ids::BLE_ID;

            fn deserialise(buf: &mut dyn Buf) -> Result<HeartbeatMsg, SerError> {
                match buf.get_u8() {
                    HB_REQ_ID => {
                        let round = buf.get_u64();
                        let n = buf.get_u64();
                        let pid = buf.get_u64();
                        let max_ballot = Ballot::with(n, pid);
                        let hb_req = HeartbeatRequest::with(round, max_ballot);
                        Ok(HeartbeatMsg::Request(hb_req))
                    }
                    HB_REP_ID => {
                        let sender_pid = buf.get_u64();
                        let round = buf.get_u64();
                        let n = buf.get_u64();
                        let pid = buf.get_u64();
                        let max_ballot = Ballot::with(n, pid);
                        let hb_rep = HeartbeatReply::with(sender_pid, round, max_ballot);
                        Ok(HeartbeatMsg::Reply(hb_rep))
                    }
                    _ => {
                        Err(SerError::InvalidType(
                            "Found unkown id but expected HeartbeatMessage".into(),
                        ))
                    }
                }
            }
        }
    }
}

/*** Shared Messages***/
#[derive(Clone, Debug)]
pub struct Run;

#[derive(Debug)]
pub struct KillResponse;

pub const RECONFIG_ID: u64 = 0;

#[derive(Clone, Debug)]
pub struct Proposal {
    pub data: Vec<u8>,
    pub reconfig: Option<(Vec<u64>, Vec<u64>)>,
}

impl Proposal {
    pub fn reconfiguration(data: Vec<u8>, reconfig: (Vec<u64>, Vec<u64>)) -> Proposal {
        let proposal = Proposal {
            data,
            reconfig: Some(reconfig),
        };
        proposal
    }

    pub fn normal(data: Vec<u8>) -> Proposal {
        let proposal = Proposal {
            data,
            reconfig: None,
        };
        proposal
    }
}

#[derive(Clone, Debug)]
pub struct ProposalResp {
    pub data: Vec<u8>,
    pub latest_leader: u64,
}

impl ProposalResp {
    pub fn with(data: Vec<u8>, latest_leader: u64) -> ProposalResp {
        ProposalResp{ data, latest_leader }
    }
}

#[derive(Clone, Debug)]
pub enum AtomicBroadcastMsg {
    Proposal(Proposal),
    ProposalResp(ProposalResp),
    FirstLeader(u64)
}

const PROPOSAL_ID: u8 = 1;
const PROPOSALRESP_ID: u8 = 2;
const FIRSTLEADER_ID: u8 = 3;

impl Serialisable for AtomicBroadcastMsg {
    fn ser_id(&self) -> u64 {
        serialiser_ids::ATOMICBCAST_ID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(50)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            AtomicBroadcastMsg::Proposal(p) => {
                buf.put_u8(PROPOSAL_ID);
                let data_len = p.data.len() as u32;
                buf.put_u32(data_len);
                buf.put_slice(p.data.as_slice());
                match &p.reconfig {
                    Some((voters, followers)) => {
                        let voters_len: u32 = voters.len() as u32;
                        buf.put_u32(voters_len);
                        for voter in voters.to_owned() {
                            buf.put_u64(voter);
                        }
                        let followers_len: u32 = followers.len() as u32;
                        buf.put_u32(followers_len);
                        for follower in followers.to_owned() {
                            buf.put_u64(follower);
                        }
                    },
                    None => {
                        buf.put_u32(0);
                        buf.put_u32(0);
                    }
                }
            },
            AtomicBroadcastMsg::ProposalResp(pr) => {
                buf.put_u8(PROPOSALRESP_ID);
                let data_len = pr.data.len() as u32;
                buf.put_u32(data_len);
                buf.put_slice(pr.data.as_slice());
                buf.put_u64(pr.latest_leader);
            },
            AtomicBroadcastMsg::FirstLeader(pid) => {
                buf.put_u8(FIRSTLEADER_ID);
                buf.put_u64(*pid);
            }
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

pub struct AtomicBroadcastDeser;

impl Deserialiser<AtomicBroadcastMsg> for AtomicBroadcastDeser {
    const SER_ID: u64 = serialiser_ids::ATOMICBCAST_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<AtomicBroadcastMsg, SerError> {
        match buf.get_u8(){
            PROPOSAL_ID => {
                let data_len = buf.get_u32() as usize;
                let mut data = vec![0; data_len];
                buf.copy_to_slice(&mut data);
                let voters_len = buf.get_u32() as usize;
                let mut voters = Vec::with_capacity(voters_len);
                for _ in 0..voters_len { voters.push(buf.get_u64()); }
                let followers_len = buf.get_u32() as usize;
                let mut followers = Vec::with_capacity(followers_len);
                for _ in 0..followers_len { followers.push(buf.get_u64()); }
                let reconfig =
                    if voters_len == 0 && followers_len == 0 {
                        None
                    } else {
                        Some((voters, followers))
                    };
                let proposal = Proposal {
                    data,
                    reconfig
                };
                Ok(AtomicBroadcastMsg::Proposal(proposal))
            },
            PROPOSALRESP_ID => {
                let data_len = buf.get_u32() as usize;
                let mut data = vec![0; data_len];
                buf.copy_to_slice(&mut data);
                let last_leader = buf.get_u64();
                let pr = ProposalResp {
                    data,
                    latest_leader: last_leader,
                };
                Ok(AtomicBroadcastMsg::ProposalResp(pr))
            },
            FIRSTLEADER_ID => {
                let pid = buf.get_u64();
                Ok(AtomicBroadcastMsg::FirstLeader(pid))
            }
            _ => {
                Err(SerError::InvalidType(
                    "Found unkown id but expected RaftMsg, Proposal or ProposalResp".into(),
                ))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum StopMsg {
    Peer(u64),
    Client
}

const PEER_STOP_ID: u8 = 1;
const CLIENT_STOP_ID: u8 = 2;

impl Serialisable for StopMsg {
    fn ser_id(&self) -> u64 { serialiser_ids::STOP_ID }

    fn size_hint(&self) -> Option<usize> {
        Some(9)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            StopMsg::Peer(pid) => {
                buf.put_u8(PEER_STOP_ID);
                buf.put_u64(*pid);
            },
            StopMsg::Client => buf.put_u8(CLIENT_STOP_ID),
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

pub struct StopMsgDeser;

impl Deserialiser<StopMsg> for StopMsgDeser {
    const SER_ID: u64 = serialiser_ids::STOP_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<StopMsg, SerError> {
        match buf.get_u8() {
            PEER_STOP_ID => {
                let pid = buf.get_u64();
                Ok(StopMsg::Peer(pid))
            },
            CLIENT_STOP_ID => Ok(StopMsg::Client),
            _ => {
                Err(SerError::InvalidType(
                    "Found unkown id but expected Peer stop or client stop".into(),
                ))
            }
        }
    }
}


#[derive(Clone, Debug)]
pub struct SequenceResp {
    pub node_id: u64,
    pub sequence: Vec<u64>
}

impl SequenceResp {
    pub fn with(node_id: u64, sequence: Vec<u64>) -> SequenceResp {
        SequenceResp{ node_id, sequence }
    }
}

#[derive(Clone, Debug)]
pub enum TestMessage {
    SequenceReq,
    SequenceResp(SequenceResp)
}

pub struct TestMessageSer;

const SEQREQ_ID: u8 = 0;
const SEQRESP_ID: u8 = 1;

impl Serialiser<TestMessage> for TestMessageSer {
    fn ser_id(&self) -> u64 {
        serialiser_ids::TEST_SEQ_ID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(50000)
    }

    fn serialise(&self, msg: &TestMessage, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match msg {
            TestMessage::SequenceReq => {
                buf.put_u8(SEQREQ_ID);
                Ok(())
            },
            TestMessage::SequenceResp(sr) => {
                buf.put_u8(SEQRESP_ID);
                buf.put_u64(sr.node_id);
                let seq_len = sr.sequence.len() as u32;
                buf.put_u32(seq_len);
                for i in &sr.sequence {
                    buf.put_u64(i.clone());
                }
                Ok(())
            }
        }
    }
}

impl Deserialiser<TestMessage> for TestMessageSer {
    const SER_ID: u64 = serialiser_ids::TEST_SEQ_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<TestMessage, SerError> {
        match buf.get_u8() {
            SEQREQ_ID => Ok(TestMessage::SequenceReq),
            SEQRESP_ID => {
                let node_id = buf.get_u64();
                let sequence_len = buf.get_u32();
                let mut sequence: Vec<u64> = Vec::new();
                for _ in 0..sequence_len {
                    sequence.push(buf.get_u64());
                }
                let sr = SequenceResp{ node_id, sequence};
                Ok(TestMessage::SequenceResp(sr))
            },
            _ => {
                Err(SerError::InvalidType(
                    "Found unkown id when deserialising TestMessage. Expected SequenceReq or SequenceResp".into(),
                ))
            }
        }
    }
}
