use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use std::collections::{HashMap, HashSet};
use super::messages::{Proposal, AtomicBroadcastMsg, AtomicBroadcastSer, Run, RECONFIG_ID};
use std::time::Duration;
use super::parameters::client::*;

#[derive(ComponentDefinition)]
pub struct Client {
    ctx: ComponentContext<Self>,
    num_proposals: u64,
    batch_size: u64,
    nodes: HashMap<u64, ActorPath>,
    reconfig: Option<(Vec<u64>, Vec<u64>)>,
    finished_latch: Arc<CountdownEvent>,
    proposal_count: u64,
    responses: HashSet<u64>,
    timer: Option<ScheduledTimer>,
    reconfig_timer: Option<ScheduledTimer>,
    timeout: u64,
    last_leader: u64,
}

impl Client {
    pub fn with(
        num_proposals: u64,
        batch_size: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        finished_latch: Arc<CountdownEvent>,
    ) -> Client {
        Client {
            ctx: ComponentContext::new(),
            num_proposals,
            batch_size,
            nodes,
            reconfig,
            finished_latch,
            proposal_count: 0,
            responses: HashSet::with_capacity(num_proposals as usize),
            timer: None,
            reconfig_timer: None,
            timeout: PROPOSAL_TIMEOUT,
            last_leader: MASTER_PID,
        }
    }

    fn propose_normal(&mut self, id: u64) {
        let p = Proposal::normal(id);
        let node = self.nodes.get(&self.last_leader).expect("Could not find actorpath to raft node!");
        node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
    }

    fn send_batch(&mut self) {
        let from = self.proposal_count + 1;
        let i = self.proposal_count + self.batch_size;
        let to = if i > self.num_proposals {
            self.num_proposals
        } else {
            i
        };
        debug!(self.ctx.log(), "Proposing {}-{}", from, to);
        for id in from ..= to {
            self.propose_normal(id);
        }
        let batch = from..=to;
        self.proposal_count += self.batch_size;
        let timeout = Duration::from_millis(self.timeout);
        let timer = self.schedule_once(timeout, move |c, _| c.retry_proposals(batch));
        self.timer = Some(timer);
    }

    fn propose_reconfiguration(&mut self) {
        let reconfig = self.reconfig.as_ref().expect("No reconfig");
        debug!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig));
        let p = Proposal::reconfiguration(RECONFIG_ID, reconfig.clone());
        let raft_node = self.nodes.get(&self.last_leader).expect("Could not find actorpath to raft node!");
        raft_node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
        let timeout = Duration::from_millis(self.timeout);
        let timer = self.schedule_once(timeout, move |c, _| c.retry_reconfig());
        self.reconfig_timer = Some(timer);
    }

    fn retry_proposals<T>(&mut self, check_proposals: T) where T: IntoIterator<Item = u64> {
        let mut timed_out: HashSet<u64> = HashSet::new();
        for id in check_proposals.into_iter() {
            if !self.responses.contains(&id) {
                self.propose_normal(id);
                timed_out.insert(id);
            }
        }
        let timeout = Duration::from_millis(self.timeout);
        let timer = self.schedule_once(timeout, move |c, _| c.retry_proposals(timed_out));
        self.timer = Some(timer);
    }

    fn retry_reconfig(&mut self) {
        if let Some(reconfig) = self.reconfig.as_ref() {
            let p = Proposal::reconfiguration(RECONFIG_ID, reconfig.clone());
            let raft_node = self.nodes.get(&MASTER_PID).expect("Could not find actorpath to raft node!");
            raft_node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
            let timeout = Duration::from_millis(self.timeout);
            let timer = self.schedule_once(timeout, move |c, _| c.retry_reconfig());
            self.reconfig_timer = Some(timer);
        }
    }
}

impl Provide<ControlPort> for Client {
    fn handle(&mut self, _event: <ControlPort as Port>::Request) -> () {
        // ignore
    }
}

impl Actor for Client {
    type Message = Run;

    fn receive_local(&mut self, _msg: Self::Message) -> () {
        self.send_batch();
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        let NetMessage{sender: _, receiver: _, data} = m;
        match_deser!{data; {
            am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                match am {
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        match pr.id {
                            RECONFIG_ID => {
                                if let Some(timer) = self.reconfig_timer.take() {
                                    self.cancel_timer(timer);
                                    self.reconfig_timer = None;
                                }
                                self.reconfig = None;
                                self.last_leader = MASTER_PID;  // MASTER_PID is always present in experiments
                            },
                            _ => {
                                if self.responses.insert(pr.id) {
                                    self.last_leader = pr.last_leader;
                                    let received_count = self.responses.len() as u64;
                                    if received_count == self.num_proposals {
                                        if let Some(timer) = self.timer.take() {
                                            self.cancel_timer(timer);
                                        }
                                        self.finished_latch.decrement().expect("Failed to countdown finished latch");
                                    } else {
                                        if received_count == self.num_proposals/2 && self.reconfig.is_some(){
                                            self.propose_reconfiguration();
                                        }
                                        if received_count % self.batch_size == 0 {
                                            if let Some(timer) = self.timer.take() {
                                                self.cancel_timer(timer);
                                            }
                                            self.send_batch();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => error!(self.ctx.log(), "Client received unexpected msg"),
                }
            },
            !Err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
        }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::bench::atomic_broadcast::messages::{TestMessage, TestMessageSer};

    #[derive(ComponentDefinition)]
    pub struct TestClient {
        ctx: ComponentContext<Self>,
        num_proposals: u64,
        batch_size: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        test_results: HashMap<u64, Vec<u64>>,
        finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
        check_sequences: bool,
        responses: HashSet<u64>,
        proposal_timeouts: HashMap<u64, ScheduledTimer>,
    }

    impl TestClient {
        pub fn with(
            num_proposals: u64,
            batch_size: u64,
            nodes: HashMap<u64, ActorPath>,
            reconfig: Option<(Vec<u64>, Vec<u64>)>,
            finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
            check_sequences: bool,
        ) -> TestClient {
            let mut tr = HashMap::new();
            tr.insert(0, Vec::with_capacity(num_proposals as usize));
            TestClient {
                ctx: ComponentContext::new(),
                num_proposals,
                batch_size,
                nodes,
                reconfig,
                test_results: tr,
                finished_promise,
                check_sequences,
                responses: HashSet::with_capacity(num_proposals as usize),
                proposal_timeouts: HashMap::with_capacity(batch_size as usize),
            }
        }

        fn propose_normal(&mut self, id: u64) {
            let p = Proposal::normal(id);
            let node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
            let timeout = Duration::from_millis(PROPOSAL_TIMEOUT);
            let timer = self.schedule_once(timeout, move |c, _| c.retry_proposal(id));
            self.proposal_timeouts.insert(id, timer);
        }

        fn send_normal_proposals<T>(&mut self, r: T) where T: IntoIterator<Item = u64> {
            for id in r {
                self.propose_normal(id);
            }
        }

        fn send_batch(&mut self) {
            let received_count = self.responses.len() as u64;
            let from = received_count + 1;
            let i = received_count + self.batch_size;
            let to = if i > self.num_proposals {
                self.num_proposals
            } else {
                i
            };
            self.send_normal_proposals(from..=to);
        }

        fn propose_reconfiguration(&mut self) {
            let reconfig = self.reconfig.take().unwrap();
            info!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig));
            let p = Proposal::reconfiguration(0, reconfig);
            let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            raft_node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
        }

        fn retry_proposal(&mut self, id: u64) {
            if !self.responses.contains(&id) {
                self.propose_normal(id);
            }
        }
    }

    impl Provide<ControlPort> for TestClient {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            match event {
                ControlEvent::Start => info!(self.ctx.log(), "Started bcast client"),
                _ => {}, //ignore
            }
        }
    }

    impl Actor for TestClient {
        type Message = Run;

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            // info!(self.ctx.log(), "CLIENT ACTORPATH={:?}", self.ctx.actor_path());
            self.send_batch();
            self.schedule_periodic(
                Duration::from_secs(5),
                Duration::from_secs(30),
                move |c, _| info!(c.ctx.log(), "Client: received: {}/{}", c.responses.len(), c.num_proposals)
            );
        }

        fn receive_network(&mut self, msg: NetMessage) -> () {
            let NetMessage{sender: _, receiver: _, data} = msg;
            match_deser!{data; {
            am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                match am {
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                            match pr.id {
                                RECONFIG_ID => {
                                    info!(self.ctx.log(), "reconfiguration succeeded?");
                                    if self.responses.len() as u64 == self.num_proposals {
                                        if self.check_sequences {
                                            info!(self.ctx.log(), "TestClient requesting sequences");
                                            for (_, actorpath) in &self.nodes {  // get sequence of ALL (past or present) nodes
                                                actorpath.tell((TestMessage::SequenceReq, TestMessageSer), self);
                                            }
                                        } else {
                                            self.finished_promise
                                                .to_owned()
                                                .fulfil(self.test_results.clone())
                                                .expect("Failed to fulfill finished promise after successful reconfiguration");
                                        }
                                    } /*else {
                                        self.send_batch();
                                    }*/
                                },
                                _ => {
                                    if self.responses.insert(pr.id) {
                                        if pr.id % 100 == 0 {
                                            info!(self.ctx.log(), "Got succeeded proposal {}", pr.id);
                                        }
                                        if let Some(timer) = self.proposal_timeouts.remove(&pr.id) {
                                            self.cancel_timer(timer);
                                        }
                                        let decided_sequence = self.test_results.get_mut(&0).unwrap();
                                        decided_sequence.push(pr.id);
                                        let received_count = self.responses.len() as u64;
    //                                    info!(self.ctx.log(), "Got proposal response id: {}", &pr.id);
                                        if received_count == self.num_proposals {
                                            if self.check_sequences {
                                                info!(self.ctx.log(), "TestClient requesting sequences. num_responses: {}", self.responses.len());
                                                for (_, actorpath) in &self.nodes {  // get sequence of ALL (past or present) nodes
                                                    actorpath.tell((TestMessage::SequenceReq, TestMessageSer), self);
                                                }
                                            } else {
                                                self.finished_promise
                                                    .to_owned()
                                                    .fulfil(self.test_results.clone())
                                                    .expect("Failed to fulfill finished promise after getting all sequences");
                                            }
                                        } else if received_count % self.batch_size == 0 {
                                            if received_count >= self.num_proposals/2 && self.reconfig.is_some() {
                                                self.propose_reconfiguration();
                                            }
                                            self.send_batch();
                                        }
                                    }
                                }
                            }
                    },
                    _ => error!(self.ctx.log(), "Client received unexpected msg"),
                }
            },
            tm: TestMessage [TestMessageSer] => {
                match tm {
                    TestMessage::SequenceResp(sr) => {
                        self.test_results.insert(sr.node_id, sr.sequence);
                        if (self.test_results.len()) == self.nodes.len() + 1 {    // got sequences from everybody, we're done
                            info!(self.ctx.log(), "Got all sequences");
                            self.finished_promise
                                .to_owned()
                                .fulfil(self.test_results.clone())
                                .expect("Failed to fulfill finished promise after getting all sequences");
                        }
                    },
                    _ => error!(self.ctx.log(), "Received unexpected TestMessage {:?}", tm),
                }
            },
            !Err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
        }
        }
        }
    }
}