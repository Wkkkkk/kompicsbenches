package se.kth.benchmarks.akka.typed_bench


import akka.serialization.Serializer
import akka.util.{ByteString, Timeout}
import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}

import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.util.concurrent.{CountDownLatch, TimeUnit}

import kompics.benchmarks.benchmarks.AtomicRegisterRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.DistributedBenchmark
import se.kth.benchmarks.akka.{ActorSystemProvider, SerializerBindings, SerializerIds, TypedIterationActor, TypedIterationActorSerializer}
import se.kth.benchmarks.akka.TypedIterationActor.{DONE, INIT_ACK, IterationMessage, START}
import se.kth.benchmarks.akka.bench.AtomicRegister.{AtomicRegisterState, ClientParams, FailedPreparationException}

//import se.kth.benchmarks.akka.typed_bench.AtomicRegister.SystemSupervisor.{StartAtomicRegister, StartIterationActor, StopActors, RunIteration, OperationSucceeded}


import scala.collection.mutable
import scala.util.Try

object AtomicRegister extends DistributedBenchmark {

  case class ClientRef(ref: String)
  case class ClientParams(read_workload: Float, write_workload: Float)

  override type MasterConf = AtomicRegisterRequest
  override type ClientConf = ClientParams
  override type ClientData = ClientRef

  val serializers = SerializerBindings
    .empty()
    .addSerializer[AtomicRegisterSerializer](AtomicRegisterSerializer.NAME)
    .addBinding[RUN.type](AtomicRegisterSerializer.NAME)
    .addBinding[READ](AtomicRegisterSerializer.NAME)
    .addBinding[WRITE](AtomicRegisterSerializer.NAME)
    .addBinding[VALUE](AtomicRegisterSerializer.NAME)
    .addBinding[ACK](AtomicRegisterSerializer.NAME)
    .addSerializer[TypedIterationActorSerializer](TypedIterationActorSerializer.NAME)
    .addBinding[DONE.type](TypedIterationActorSerializer.NAME)
    .addBinding[INIT](TypedIterationActorSerializer.NAME)
    .addBinding[INIT_ACK](TypedIterationActorSerializer.NAME)

  class MasterImpl extends Master {
    import SystemSupervisor._

    private var read_workload = 0.0F;
    private var write_workload = 0.0F;
    private var partition_size: Int = -1;
    private var num_keys: Long = -1l;
//    private var system: ActorSystem = null;
    private var system: ActorSystem[SystemSupervisor.SystemMessage] = null;
    private var prepare_latch: CountDownLatch = null;
    private var finished_latch: CountDownLatch = null;
    private var init_id: Int = -1;

    override def setup(c: MasterConf): ClientConf = {
      println("Atomic Register(Master) Setup!")
      system = ActorSystemProvider.newRemoteTypedActorSystem[SystemSupervisor.SystemMessage](SystemSupervisor(), "atomicreg_supervisor", 1, serializers)
      this.read_workload = c.readWorkload;
      this.write_workload = c.writeWorkload;
      this.partition_size = c.partitionSize;
      this.num_keys = c.numberOfKeys;
      ClientParams(read_workload, write_workload)
    };

    override def prepareIteration(d: List[ClientData]): Unit = {
      system ! StartAtomicRegister(read_workload, write_workload)
      init_id += 1
      prepare_latch = new CountDownLatch(1)
      finished_latch = new CountDownLatch(1)
      system ! StartIterationActor(prepare_latch, finished_latch, init_id, d, num_keys, partition_size)
      val timeout = 100
      val timeunit = TimeUnit.SECONDS
      val successful_prep = prepare_latch.await(timeout, timeunit)
      if (!successful_prep) {
        println("Timeout in prepareIteration for INIT_ACK")
        throw new FailedPreparationException("Timeout waiting for INIT ACK from all nodes")
      }

    }

    override def runIteration(): Unit = {
      system ! RunIteration
      finished_latch.await()
    };

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up Atomic Register(Master) side");
      if (prepare_latch != null) prepare_latch = null
      if (finished_latch != null) finished_latch = null
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[OperationSucceeded.type] = system.ask(ref => StopActors(ref))
      implicit val ec = system.executionContext
      Await.result(f, 5 seconds)
      if (lastIteration) {
        println("Cleaning up Last iteration")
//        system.terminate()
        system ! GracefulShutdown
        Await.ready(system.whenTerminated, 5 seconds)
        system = null
        println("Last clean up completed")
      }
    }

    object SystemSupervisor {
      def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context))

      sealed trait SystemMessage

      case class StartIterationActor(prepare_latch: CountDownLatch, finished_latch: CountDownLatch, init_id: Int, nodes: List[ClientRef], num_keys: Long, partition_size: Int) extends SystemMessage
      case class StartAtomicRegister(read_workload: Float, write_workload: Float) extends SystemMessage
      case class StopActors(ref: ActorRef[OperationSucceeded.type]) extends SystemMessage
      case object RunIteration extends SystemMessage
      case object GracefulShutdown extends SystemMessage
      case object OperationSucceeded
    }

    class SystemSupervisor(context: ActorContext[SystemMessage]) extends AbstractBehavior[SystemMessage]{

      val resolver = ActorRefResolver(context.system)
      var atomicRegister: ActorRef[AtomicRegisterMessage] = null
      var iterationActor: ActorRef[IterationMessage] = null

      override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
        msg match {
          case StartAtomicRegister(r, w) => {
            atomicRegister = context.spawn[AtomicRegisterMessage](AtomicRegisterActor(r, w), "typed_atomicreg".concat(init_id.toString))
            println(s"Atomic Register(Master) ref is ${resolver.toSerializationFormat(atomicRegister)}")
            this
          }
          case s: StartIterationActor => {
            val atomicRegRef = ClientRef(resolver.toSerializationFormat(atomicRegister))
            val nodes = atomicRegRef :: s.nodes
            val num_nodes = nodes.size
            assert(partition_size <= num_nodes && partition_size > 0 && read_workload + write_workload == 1)
            iterationActor = context.spawn[IterationMessage](TypedIterationActor(s.prepare_latch, s.finished_latch, s.init_id, nodes, s.num_keys, s.partition_size), "typed_itactor")
            iterationActor ! START
            this
          }
          case StopActors(ref) => {
            if (atomicRegister != null){
              context.stop(atomicRegister)
              atomicRegister = null
              context.stop(iterationActor)
              iterationActor = null
            }
            ref ! OperationSucceeded
            this
          }
          case RunIteration => {
            iterationActor ! TypedIterationActor.RUN
            this
          }
          case GracefulShutdown => {
            context.log.info("Graceful shutdown of SystemSupervisor for Atomic Register")
            Behaviors.stopped
          }
        }
      }
    }
  }

  class ClientImpl extends Client {
    private var read_workload = 0.0F;
    private var write_workload = 0.0F;
    private var system: ActorSystem[AtomicRegisterMessage] = null

    override def setup(c: ClientConf): ClientData = {
      println("Atomic Register(Client) Setup!")
      this.read_workload = c.read_workload
      this.write_workload = c.write_workload
      system = ActorSystemProvider.newRemoteTypedActorSystem[AtomicRegisterMessage](AtomicRegisterActor(read_workload, write_workload), "atomicRegister", 1, serializers)
      val resolver = ActorRefResolver(system)
      ClientRef(resolver.toSerializationFormat(system))
    }

    override def prepareIteration(): Unit = {
      println("Preparing Atomic Register(Client) iteration")
    }

    override def cleanupIteration(lastIteration: Boolean): Unit = {
      println("Cleaning up Atomic Register(Client) side")
      if (lastIteration){
        println("Cleaning up Last iteration")
//        system.terminate()
        system ! STOP
        Await.ready(system.whenTerminated, 5 seconds)
        system = null
        println("Last clean up completed")
      }
    }
  }

  override def newMaster(): Master = new MasterImpl()

  override def msgToMasterConf(msg: GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[AtomicRegisterRequest]
  };

  override def newClient(): Client = new ClientImpl()

  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(":");
    assert(split.length == 2);
    ClientParams(split(0).toFloat, split(1).toFloat)
  }

  override def strToClientData(str: String): Try[ClientData] = Success(ClientRef(str))

  override def clientConfToString(c: ClientConf): String = s"${c.read_workload}:${c.write_workload}";

  override def clientDataToString(d: ClientData): String = d.ref


  trait AtomicRegisterMessage

  case object RUN extends AtomicRegisterMessage
  case object STOP extends AtomicRegisterMessage
  case class INIT(src: ClientRef, rank: Int, init_id: Int, nodes: List[ClientRef], min: Long, max: Long) extends AtomicRegisterMessage
  case class READ(src: ClientRef, run_id: Int, key: Long, rid: Int) extends AtomicRegisterMessage
  case class VALUE(src: ClientRef, run_id: Int, key: Long, rid: Int, ts: Int, wr: Int, value: Int) extends AtomicRegisterMessage
  case class WRITE(src: ClientRef, run_id: Int, key: Long, rid: Int, ts: Int, wr: Int, value: Int) extends AtomicRegisterMessage
  case class ACK(run_id: Int, key: Long, rid: Int) extends AtomicRegisterMessage

  object AtomicRegisterActor {
    def apply(read_workload: Float, write_workload: Float): Behavior[AtomicRegisterMessage] = Behaviors.setup(context => new AtomicRegisterActor(context, read_workload, write_workload))
  }

  class AtomicRegisterActor(context: ActorContext[AtomicRegisterMessage], read_workload: Float, write_workload: Float) extends AbstractBehavior[AtomicRegisterMessage] {
    implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.Ops = o.mkOrderingOps(x); // for tuple comparison
//    val logger = context.log

    var nodes: List[ActorRef[AtomicRegisterMessage]] = _
    var n = 0
    var selfRank: Int = -1
    var register_state: mutable.Map[Long, AtomicRegisterState] = mutable.Map.empty // (key, state)
    var register_readlist: mutable.Map[Long, mutable.Map[ActorRef[AtomicRegisterMessage], (Int, Int, Int)]] = mutable.Map.empty

    var min_key: Long = -1
    var max_key: Long = -1

    /* Experiment variables */
    var read_count: Long = 0
    var write_count: Long = 0
    var master: ActorRef[IterationMessage] = _
    var current_run_id: Int = -1

    val resolver = ActorRefResolver(context.system)
    val selfRef = ClientRef(resolver.toSerializationFormat(context.self))

    private def getActorRef[T](c: ClientRef): ActorRef[T] = {
      resolver.resolveActorRef(c.ref)
    }

    private def bcast(receivers: List[ActorRef[AtomicRegisterMessage]], msg: AtomicRegisterMessage): Unit = {
      for (node <- receivers) node ! msg
    }

    private def invokeRead(key: Long): Unit = {
      val register = register_state(key)
      register.rid += 1;
      register.acks = 0
      register_readlist(key).clear()
      register.reading = true
      bcast(nodes, READ(selfRef, current_run_id, key, register.rid))
    }

    private def invokeWrite(key: Long): Unit = {
      val wval = selfRank
      val register = register_state(key)
      register.rid += 1
      register.writeval = wval
      register.acks = 0
      register.reading = false
      register_readlist(key).clear()
      bcast(nodes, READ(selfRef, current_run_id, key, register.rid))
    }

    private def invokeOperations(): Unit = {
      val num_keys = max_key - min_key + 1
      val num_reads = (num_keys * read_workload).toLong
      val num_writes = (num_keys * write_workload).toLong

      read_count = num_reads
      write_count = num_writes

      if (selfRank % 2 == 0) {
        for (i <- 0l until num_reads) invokeRead(min_key + i)
        for (i <- 0l until num_writes) invokeWrite(min_key + num_reads + i)
      } else {
        for (i <- 0l until num_writes) invokeWrite(min_key + i)
        for (i <- 0l until num_reads) invokeRead(min_key + num_writes + i)
      }
    }

    private def readResponse(key: Long, read_value: Int): Unit = {
      read_count -= 1
      if (read_count == 0 && write_count == 0) master ! DONE

    }

    private def writeResponse(key: Long): Unit = {
      write_count -= 1
      if (read_count == 0 && write_count == 0) master ! DONE
    }

    override def onMessage(msg: AtomicRegisterMessage): Behavior[AtomicRegisterMessage] = {
      msg match {
        case i: INIT => {
          current_run_id = i.init_id
          nodes = for (node <- i.nodes) yield getActorRef[AtomicRegisterMessage](node)
          n = i.nodes.size
          selfRank = i.rank
          min_key = i.min
          max_key = i.max
          /* Reset KV and states */
          register_state.clear()
          register_readlist.clear()
          for (i <- min_key to max_key) {
            register_state += (i -> new AtomicRegisterState)
            register_readlist += (i -> mutable.Map.empty[ActorRef[AtomicRegisterMessage], (Int, Int, Int)])
          }
          master = getActorRef[IterationMessage](i.src)
          master ! INIT_ACK(current_run_id)
        }

        case RUN => {
          invokeOperations()
        }

        case STOP => {
          context.log.info("Stopping atomic register")
          return Behaviors.stopped
        }

        case READ(src, current_run_id, key, readId) => {
          val current_state: AtomicRegisterState = register_state(key)
          getActorRef(src) ! VALUE(selfRef, current_run_id, key, readId, current_state.ts, current_state.wr, current_state.value)
        }

        case v: VALUE => {
          if (v.run_id == current_run_id) {
            val current_register = register_state(v.key)
            if (v.rid == current_register.rid) {
              var readlist = register_readlist(v.key)
              if (current_register.reading) {
                if (readlist.isEmpty) {
                  current_register.first_received_ts = v.ts
                  current_register.readval = v.value
                } else if (current_register.skip_impose) {
                  if (current_register.first_received_ts != v.ts) current_register.skip_impose = false
                }
              }
              val src = getActorRef[AtomicRegisterMessage](v.src)
              readlist(src) = (v.ts, v.wr, v.value)
              if (readlist.size > n / 2) {
                if (current_register.reading && current_register.skip_impose) {
                  current_register.value = current_register.readval
                  register_readlist(v.key).clear()
                  readResponse(v.key, current_register.readval)
                } else {
                  var (maxts, rr, readvalue) = readlist.values.maxBy(_._1)
                  current_register.readval = readvalue
                  register_readlist(v.key).clear()
                  var bcastvalue = readvalue
                  if (!current_register.reading) {
                    rr = selfRank
                    maxts += 1
                    bcastvalue = current_register.writeval
                  }
                  bcast(nodes, WRITE(selfRef, v.run_id, v.key, v.rid, maxts, rr, bcastvalue))
                }
              }
            }
          }
        }

        case w: WRITE => {
          if (w.run_id == current_run_id) {
            val current_state = register_state(w.key)
            if ((w.ts, w.wr) > (current_state.ts, current_state.wr)) {
              current_state.ts = w.ts
              current_state.wr = w.wr
              current_state.value = w.value
            }
          }
          getActorRef(w.src) ! ACK(w.run_id, w.key, w.rid)
        }

        case a: ACK => {
          if (a.run_id == current_run_id) {
            val current_register = register_state(a.key)
            if (a.rid == current_register.rid) {
              current_register.acks += 1
              if (current_register.acks > n / 2) {
                register_state(a.key).acks = 0
                if (current_register.reading) {
                  readResponse(a.key, current_register.readval)
                } else {
                  writeResponse(a.key)
                }
              }
            }
          }
        }
      }
      this
    }
  }

  object AtomicRegisterSerializer {
    val NAME = "typed_atomicregister"

    private val READ_FLAG: Byte = 1
    private val WRITE_FLAG: Byte = 2
    private val ACK_FLAG: Byte = 3
    private val VALUE_FLAG: Byte = 4
    private val RUN_FLAG: Byte = 5
  }

  class AtomicRegisterSerializer extends Serializer {
    import AtomicRegisterSerializer._
    import java.nio.{ ByteBuffer, ByteOrder }

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.TYPEDATOMICREG
    override def includeManifest: Boolean = false

    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case r: READ => {
          val br = ByteString.createBuilder.putByte(READ_FLAG)
          val src_bytes = r.src.ref.getBytes
          br.putShort(src_bytes.size)
          br.putBytes(src_bytes)
          br.putInt(r.run_id).putLong(r.key).putInt(r.rid).result().toArray
        }
        case w: WRITE => {
          val br = ByteString.createBuilder.putByte(WRITE_FLAG)
          val src_bytes = w.src.ref.getBytes
          br.putShort(src_bytes.size)
          br.putBytes(src_bytes)
          br.putInt(w.run_id).putLong(w.key).putInt(w.rid).putInt(w.ts).putInt(w.wr).putInt(w.value).result().toArray
        }
        case v: VALUE => {
          val br = ByteString.createBuilder.putByte(VALUE_FLAG)
          val src_bytes = v.src.ref.getBytes
          br.putShort(src_bytes.size)
          br.putBytes(src_bytes)
          br.putInt(v.run_id).putLong(v.key).putInt(v.rid).putInt(v.ts).putInt(v.wr).putInt(v.value).result().toArray
        }
        case a: ACK => {
          ByteString.createBuilder.putByte(ACK_FLAG).putInt(a.run_id).putLong(a.key).putInt(a.rid).result().toArray
        }
        case RUN => Array(RUN_FLAG)
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;
      flag match {
        case READ_FLAG => {
          val src_length: Int = buf.getShort
          val src_bytes = new Array[Byte](src_length)
          buf.get(src_bytes)
          val src_ref = ClientRef(src_bytes.map(_.toChar).mkString)
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          READ(src_ref, run_id, key, rid)
        }
        case WRITE_FLAG => {
          val src_length: Int = buf.getShort
          val src_bytes = new Array[Byte](src_length)
          buf.get(src_bytes)
          val src_ref = ClientRef(src_bytes.map(_.toChar).mkString)
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          val ts = buf.getInt
          val wr = buf.getInt
          val value = buf.getInt
          WRITE(src_ref, run_id, key, rid, ts, wr, value)
        }
        case VALUE_FLAG => {
          val src_length: Int = buf.getShort
          val src_bytes = new Array[Byte](src_length)
          buf.get(src_bytes)
          val src_ref = ClientRef(src_bytes.map(_.toChar).mkString)
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          val ts = buf.getInt
          val wr = buf.getInt
          val value = buf.getInt
          VALUE(src_ref, run_id, key, rid, ts, wr, value)
        }
        case ACK_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          ACK(run_id, key, rid)
        }
        case RUN_FLAG => RUN
      }
    }

  }



}

