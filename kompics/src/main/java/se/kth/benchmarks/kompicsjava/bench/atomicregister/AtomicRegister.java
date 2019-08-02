package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.*;
import se.kth.benchmarks.kompicsjava.broadcast.BEBDeliver;
import se.kth.benchmarks.kompicsjava.broadcast.BEBRequest;
import se.kth.benchmarks.kompicsjava.broadcast.BestEffortBroadcast;
import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;

import se.kth.benchmarks.kompicsscala.KompicsSystemProvider;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class AtomicRegister extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<AtomicRegister> {
        private final float read_workload;
        private final float write_workload;

        public Init(float read_workload, float write_workload) {
            this.read_workload = read_workload;
            this.write_workload = write_workload;
        }
    }

    private Positive<BestEffortBroadcast> beb = requires(BestEffortBroadcast.class);
    private Positive<Network> net = requires(Network.class);

    private int selfRank;
    private final NetAddress selfAddr;
    private List<NetAddress> nodes;
    private int N;
    private long min_key = -1;
    private long max_key = -1;
    private HashMap<Long, AtomicRegisterState> register_state = new HashMap<>();
    private HashMap<Long, HashMap<NetAddress, Tuple>> register_readList = new HashMap<>();

    // benchmark variables
    private long read_count;
    private long write_count;
    private float read_workload;
    private float write_workload;
    private NetAddress master;
    private int current_run_id = -1;

    public AtomicRegister(Init init) {
        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);
        read_workload = init.read_workload;
        write_workload = init.write_workload;
        subscribe(startHandler, control);
        subscribe(readRequestHandler, beb);
        subscribe(writeRequestHandler, beb);
        subscribe(readResponseHandler, net);
        subscribe(ackHandler, net);
        subscribe(initHandler, net);
        subscribe(runHandler, net);
    }

    private void newIteration(INIT i){
        current_run_id = i.id;
        nodes = i.nodes;
        N = nodes.size();
        selfRank = i.rank;
        min_key = i.min;
        max_key = i.max;
        register_state.clear();
        register_readList.clear();
        for (long l = min_key; l <= max_key; l++){
            register_state.put(l, new AtomicRegisterState());
            register_readList.put(l, new HashMap<>());
        }
    }

    private void invokeRead(long key){
        AtomicRegisterState register = register_state.get(key);
        register.rid++;
        register.acks = 0;
        register_readList.get(key).clear();
        register.reading = true;
//        logger.info("Invoking read key=" + key);
        trigger(new BEBRequest(nodes, new READ(current_run_id, key, register.rid)), beb);
    }

    private void invokeWrite(long key){
        int wval = selfRank;
        AtomicRegisterState register = register_state.get(key);
        register.rid++;
        register.writeval = wval;
        register.acks = 0;
        register.reading = false;
        register_readList.get(key).clear();
//        logger.info("Invoking write key=" + key);
        trigger(new BEBRequest(nodes, new READ(current_run_id, key, register.rid)), beb);
    }

    private void invokeOperations(){
        long num_keys = max_key - min_key + 1;
        long num_reads = (long) (num_keys * read_workload);
        long num_writes = (long) (num_keys * write_workload);
//        logger.info("Invoke operations: " + num_reads + " reads, " + num_writes + " writes");
        read_count = num_reads;
        write_count = num_writes;

        if (selfRank % 2 == 0){
            for (long l = 0; l < num_reads; l++) invokeRead(min_key + l);
            for (long l = 0; l < num_writes; l++) invokeWrite(min_key + num_reads + l);
        } else {
            for (long l = 0; l < num_writes; l++) invokeWrite(min_key + l);
            for (long l = 0; l < num_reads; l++) invokeRead(min_key + num_writes + l);
        }
    }

    private void runFinished(){
        logger.info("Atomic Register " + selfAddr.asString() + " is done!");
        trigger(se.kth.benchmarks.kompicsscala.NetMessage.viaTCP(selfAddr.asScala(), master.asScala(), DONE.event), net);
    }

    private void readResponse(long key, int read_value){
        read_count--;
//        logger.info("Read response: key=" + key + ", read_count=" + read_count); // + ", value=" + read_value);
        if (read_count == 0 && write_count == 0) runFinished();

    }

    private void writeResponse(long key){
        write_count--;
//        logger.info("Write response: key=" + key + ", write_count=" + write_count);
        if (read_count == 0 && write_count == 0) runFinished();
    }

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            assert(selfAddr != null);
//            logger.info("Atomic Register Component " + selfAddr.asString() + " has started!");
        }

    };

    private ClassMatchedHandler<READ, BEBDeliver> readRequestHandler = new ClassMatchedHandler<READ, BEBDeliver>() {
        @Override
        public void handle(READ read, BEBDeliver bebDeliver) {
            if (read.run_id == current_run_id) {
                AtomicRegisterState current_state = register_state.get(read.key);
//                logger.info("Responding to read with VALUE, key=" + read.key + " to " + bebDeliver.src.asString());
                trigger(NetMessage.viaTCP(selfAddr, bebDeliver.src, new VALUE(read.run_id, read.key, read.rid, current_state.ts, current_state.wr, current_state.value)), net);
            }
        }
    };

    private ClassMatchedHandler<WRITE, BEBDeliver> writeRequestHandler = new ClassMatchedHandler<WRITE, BEBDeliver>() {
        @Override
        public void handle(WRITE write, BEBDeliver bebDeliver) {
            if (write.run_id == current_run_id){
                AtomicRegisterState current_state = register_state.get(write.key);
                if (write.ts > current_state.ts || (write.ts == current_state.ts && write.wr > current_state.wr)) {
                    current_state.ts = write.ts;
                    current_state.wr = write.wr;
                    current_state.value = write.value;
                }
            }
//            logger.info("Acking WRITE key=" + write.key + ", run_id=" + current_run_id + " to " + bebDeliver.src.asString());
            trigger(NetMessage.viaTCP(selfAddr, bebDeliver.src, new ACK(write.run_id, write.key, write.rid)), net);
        }
    };

    private ClassMatchedHandler<INIT, NetMessage> initHandler = new ClassMatchedHandler<INIT, NetMessage>() {
        @Override
        public void handle(INIT init, NetMessage netMessage) {
            logger.info("Got INIT id=" + init.id);
            newIteration(init);
            master = netMessage.getSource();
            trigger(se.kth.benchmarks.kompicsscala.NetMessage.viaTCP(selfAddr.asScala(), netMessage.getSource().asScala(), new INIT_ACK(init.id)), net);
        }
    };

    private ClassMatchedHandler<VALUE, NetMessage> readResponseHandler = new ClassMatchedHandler<VALUE, NetMessage>() {
        @Override
        public void handle(VALUE v, NetMessage msg) {
            if (v.run_id == current_run_id) {
                AtomicRegisterState current_register = register_state.get(v.key);
                if (v.rid == current_register.rid) {
                    HashMap<NetAddress, Tuple> readList = register_readList.get(v.key);
                    if (current_register.reading) {
                        if (readList.isEmpty()) {
                            current_register.first_received_ts = v.ts;
                            current_register.readval = v.value;
                        } else if (current_register.skip_impose) {
                            if (current_register.first_received_ts != v.ts) current_register.skip_impose = false;
                        }
                    }
                    readList.put(msg.getSource(), new Tuple(v.ts, v.wr, v.value));
                    if (readList.size() > N / 2) {
                        if (current_register.reading && current_register.skip_impose) {
//                            logger.info("Skipped impose: key=" + v.key + ",  ts=" + v.ts);
                            current_register.value = current_register.readval;
                            readList.clear();
                            readResponse(v.key, current_register.readval);
                        } else {
                            Tuple maxtuple = getMaxTuple(v.key);
                            int maxts = maxtuple.ts;
                            int rr = maxtuple.wr;
                            current_register.readval = maxtuple.value;
//                            logger.info("Trigger WRITE key=" + v.key + ", reading=" + current_register.reading + ", readList size=" + readList.size());
                            readList.clear();
                            int bcastval;
                            if (current_register.reading) {
                                bcastval = current_register.readval;
                            } else {
                                rr = selfRank;
                                maxts++;
                                bcastval = current_register.writeval;
                            }
                            trigger(new BEBRequest(nodes, new WRITE(v.run_id, v.key, v.rid, maxts, rr, bcastval)), beb);
                        }
                    }
                }
            }
        }
    };

    private ClassMatchedHandler<ACK, NetMessage> ackHandler = new ClassMatchedHandler<ACK, NetMessage>() {
        @Override
        public void handle(ACK a, NetMessage msg) {
            if (a.run_id == current_run_id) {  // avoid redundant acks from previous runs
                AtomicRegisterState current_register = register_state.get(a.key);
                if (a.rid == current_register.rid) {
                    current_register.acks++;
//                    logger.info("Got ack #" + current_register.acks + " for key=" + a.key + ", run_id=" + a.run_id + " from " + msg.getSource().asString());
                    if (current_register.acks > N / 2) {
                        current_register.acks = 0;
                        if (current_register.reading) readResponse(a.key, current_register.readval);
                        else writeResponse(a.key);
                    }
                }
            }
        }
    };

    private ClassMatchedHandler<RUN, NetMessage> runHandler = new ClassMatchedHandler<RUN, NetMessage>() {
        @Override
        public void handle(RUN run, NetMessage netMessage) {
            invokeOperations();
        }
    };

    private Tuple getMaxTuple(long key){
        Collection<Tuple> readListValues = register_readList.get(key).values();
        Tuple maxtuple = readListValues.iterator().next();
        for (Tuple t : readListValues){
            if (t.ts > maxtuple.ts){
                maxtuple = t;
            }
        }
        return maxtuple;
    }

    private class Tuple{
        int ts;     // seq nr
        int wr;     // rank
        int value;

        Tuple(int ts, int wr, int value){
            this.ts = ts;
            this.wr = wr;
            this.value = value;
        }
    }

    private class AtomicRegisterState{
        private int ts, wr;
        private int value;
        private int acks;
        private int rid;
        private Boolean reading;
        private int readval;
        private int writeval;
        private int first_received_ts = -1;
        private Boolean skip_impose = true;
    }

}