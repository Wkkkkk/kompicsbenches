use kompact::prelude::*;
use crate::serialiser_ids;
use super::messages::*;

pub struct LeaderInboundSer;
pub struct AcceptorInboundSer;
pub struct ReplicaInboundSer;
pub struct BatcherInboundSer;
pub struct ProxyLeaderInboundSer;

fn serialise_phase1b_slot_info(p1b_slot_info: &Phase1bSlotInfo, buf: &mut dyn BufMut) {
    buf.put_u64(p1b_slot_info.slot);
    buf.put_u64(p1b_slot_info.vote_round);
    match &p1b_slot_info.vote_value {
        CommandBatchOrNoop::CommandBatch(c) => {
            buf.put_u8(1);
            buf.put_u32(c.len() as u32);
            for cmd in c {
                let data = cmd.as_slice();
                buf.put_u32(data.len() as u32);
                buf.put_slice(data);
            }
        }
        CommandBatchOrNoop::Noop => {
            buf.put_u8(0)
        }
    }
}

fn deserialise_phase1b_slot_info(buf: &mut dyn Buf) -> Phase1bSlotInfo {
    let slot = buf.get_u64();
    let vote_round = buf.get_u64();
    let vote_value = match buf.get_u8() {
        0 => CommandBatchOrNoop::Noop,
        1 => {
            let len = buf.get_u32();
            let mut batch = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let data_len = buf.get_u32() as usize;
                let mut data = vec![0; data_len];
                buf.copy_to_slice(&mut data);
                batch.push(data);
            }
            CommandBatchOrNoop::CommandBatch(batch)
        },
        _ => unimplemented!("unexpected command batch value")
    };
    Phase1bSlotInfo { slot, vote_round, vote_value }
}

impl Serialisable for LeaderInbound {
    fn ser_id(&self) -> u64 {
        serialiser_ids::MP_LEADER_ID
    }

    fn size_hint(&self) -> Option<usize> {
        None
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        todo!()
        /*
        match self {
            LeaderInbound::Phase1b(p1b) => {
                buf.put_u64(p1b.round);
                buf.put_u64(p1b.acceptor_index);
                buf.put_u32(p1b.info.len() as u32);
                for info in &p1b.info {
                    serialise_phase1b_slot_info(info, buf);
                }
            }
            LeaderInbound::Nack(_) => {}
            LeaderInbound::ChosenWatermark(_) => {}
            LeaderInbound::Recover(_) => {}
            LeaderInbound::Phase2a(_) => {}
            LeaderInbound::Phase2b(_) => {}
        }
        Ok(())
        */
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        todo!()
    }
}

impl Serialisable for AcceptorInbound {
    fn ser_id(&self) -> u64 {
        todo!()
    }

    fn size_hint(&self) -> Option<usize> {
        todo!()
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        todo!()
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        todo!()
    }
}

impl Serialisable for ReplicaInbound {
    fn ser_id(&self) -> u64 {
        todo!()
    }

    fn size_hint(&self) -> Option<usize> {
        todo!()
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        todo!()
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        todo!()
    }
}

impl Serialisable for BatcherInbound {
    fn ser_id(&self) -> u64 {
        todo!()
    }

    fn size_hint(&self) -> Option<usize> {
        todo!()
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        todo!()
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        todo!()
    }
}

impl Serialisable for ProxyLeaderInbound {
    fn ser_id(&self) -> u64 {
        todo!()
    }

    fn size_hint(&self) -> Option<usize> {
        todo!()
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        todo!()
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        todo!()
    }
}

impl Deserialiser<LeaderInbound> for LeaderInboundSer {
    const SER_ID: SerId = 0;

    fn deserialise(buf: &mut dyn Buf) -> Result<LeaderInbound, SerError> {
        todo!()
    }
}

impl Deserialiser<AcceptorInbound> for AcceptorInboundSer {
    const SER_ID: SerId = 0;

    fn deserialise(buf: &mut dyn Buf) -> Result<AcceptorInbound, SerError> {
        todo!()
    }
}

impl Deserialiser<ReplicaInbound> for ReplicaInboundSer {
    const SER_ID: SerId = 0;

    fn deserialise(buf: &mut dyn Buf) -> Result<ReplicaInbound, SerError> {
        todo!()
    }
}

impl Deserialiser<BatcherInbound> for BatcherInboundSer {
    const SER_ID: SerId = 0;

    fn deserialise(buf: &mut dyn Buf) -> Result<BatcherInbound, SerError> {
        todo!()
    }
}

impl Deserialiser<ProxyLeaderInbound> for ProxyLeaderInboundSer {
    const SER_ID: SerId = 0;

    fn deserialise(buf: &mut dyn Buf) -> Result<ProxyLeaderInbound, SerError> {
        todo!()
    }
}




