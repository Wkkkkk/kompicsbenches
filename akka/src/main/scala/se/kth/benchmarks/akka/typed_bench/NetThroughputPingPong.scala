package se.kth.benchmarks.akka.typed_bench

import java.util.concurrent.CountDownLatch

import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern._
import akka.serialization.Serializer
import akka.util.{ByteString, Timeout}
import kompics.benchmarks.benchmarks.ThroughputPingPongRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.DistributedBenchmark
import se.kth.benchmarks.akka.bench.NetThroughputPingPong.{Ping, PingPongSerializer, Pong, StaticPing, StaticPong}
import se.kth.benchmarks.akka.{ActorSystemProvider, SerializerBindings, SerializerIds}
import se.kth.benchmarks.akka.typed_bench.NetThroughputPingPong.ClientSystemSupervisor.{ClientShutdown, ClientSystemMessage, StartPongers}
import se.kth.benchmarks.akka.typed_bench.NetThroughputPingPong.SystemSupervisor.{GracefulShutdown, OperationSucceeded, RunIteration, StartPingers, StopPingers, SystemMessage}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

object NetThroughputPingPong extends DistributedBenchmark{
  case class ActorReference(actorPath: String)
  case class ClientRefs(actorPaths: List[String])
  case class ClientParams(numPongers: Int, staticOnly: Boolean)

  override type MasterConf = ThroughputPingPongRequest;
  override type ClientConf = ClientParams;
  override type ClientData = ClientRefs;

  val serializers = SerializerBindings
    .empty()
    .addSerializer[PingPongSerializer](PingPongSerializer.NAME)
    .addBinding[StaticPing](PingPongSerializer.NAME)
    .addBinding[StaticPong.type](PingPongSerializer.NAME)
    .addBinding[Ping](PingPongSerializer.NAME)
    .addBinding[Pong](PingPongSerializer.NAME);


  class MasterImpl extends Master {
    private var numMsgs = -1l;
    private var numPairs = -1;
    private var pipeline = -1l;
    private var staticOnly = true;
    private var system: ActorSystem[SystemMessage] = null
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf): ClientConf = {
      println("NetPingPong setup!")
      system = ActorSystemProvider.newRemoteTypedActorSystem[SystemMessage](SystemSupervisor(), "nettpingpong_supervisor", Runtime.getRuntime.availableProcessors(), serializers)
      this.numMsgs = c.messagesPerPair;
      this.numPairs = c.parallelism;
      this.pipeline = c.pipelineSize;
      this.staticOnly = c.staticOnly;
      ClientParams(numPairs, staticOnly)
    }

    override def prepareIteration(d: List[ClientData]): Unit = {
      latch = new CountDownLatch(numPairs);
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[OperationSucceeded.type] = system.ask(ref => StartPingers(ref, latch, numMsgs, pipeline, staticOnly, d.head))
      implicit val ec = system.executionContext
      Await.result(f, 3 seconds)
    }

    override def runIteration(): Unit = {
      system ! RunIteration
      latch.await()
    }

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up pinger side");
      if (latch != null) {
        latch = null;
      }
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[OperationSucceeded.type] = system.ask(ref => StopPingers(ref))
      implicit val ec = system.executionContext
      Await.result(f, 3 seconds)
      if (lastIteration){
        println("Cleaning up last iteration...")
//        system.terminate()
        system ! GracefulShutdown
        Await.ready(system.whenTerminated, 5 seconds)
        system = null
        println("Last cleanup completed")
      }

    }
  }

  class ClientImpl extends Client{
    private var system: ActorSystem[ClientSystemMessage] = null

    override def setup(c: ClientParams): ClientRefs = {
      system = ActorSystemProvider.newRemoteTypedActorSystem[ClientSystemMessage](ClientSystemSupervisor(), "nettpingpong_clientsupervisor", 1, serializers)
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[ClientRefs] = system.ask(ref => StartPongers(ref, c.staticOnly, c.numPongers))
      implicit val ec = system.executionContext
      val ready = Await.ready(f, 5 seconds)
      ready.value.get match {
        case Success(pongerPaths) => {
          println(s"Ponger Paths are${pongerPaths.actorPaths.mkString}")
          pongerPaths
        }
        case Failure(e) => ClientRefs(List.empty)
      }
    }
    override def prepareIteration(): Unit = {
      // nothing
      println("Preparing ponger iteration");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      println("Cleaning up ponger side");
      if (lastIteration) {
//        system.terminate()
        system ! ClientShutdown
        Await.ready(system.whenTerminated, 5.second);
        system = null;
      }
    }
  }

  override def newMaster(): Master = new MasterImpl()

  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[ThroughputPingPongRequest]
  };

  override def newClient(): NetThroughputPingPong.Client = new ClientImpl()

  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(",");
    val num = split(0).toInt;
    val staticOnly = split(1) match {
      case "true"  => true
      case "false" => false
    };
    ClientParams(num, staticOnly)
  };
  override def strToClientData(str: String): Try[ClientData] = Try {
    val l = str.split(",").toList;
    ClientRefs(l)
  };

  override def clientConfToString(c: ClientConf): String = s"${c.numPongers},${c.staticOnly}";

  override def clientDataToString(d: ClientData): String = d.actorPaths.mkString(",");


  object ClientSystemSupervisor {
    sealed trait ClientSystemMessage

    case class StartPongers(replyTo: ActorRef[ClientRefs], staticOnly: Boolean, numPongers: Int) extends ClientSystemMessage
    case object ClientShutdown extends ClientSystemMessage
    def apply(): Behavior[ClientSystemMessage] = Behaviors.setup(context => new ClientSystemSupervisor(context))
  }

  class ClientSystemSupervisor(context: ActorContext[ClientSystemMessage]) extends AbstractBehavior[ClientSystemMessage]{
    val resolver = ActorRefResolver(context.system)

    private def getPongerPaths[T](refs: List[ActorRef[T]]): List[String] = {
      for (pongerRef <- refs) yield resolver.toSerializationFormat(pongerRef)
    }

    override def onMessage(msg: ClientSystemMessage): Behavior[ClientSystemMessage] = {
      msg match {
        case s: StartPongers => {
          if (s.staticOnly){
            val static_pongers = (1 to s.numPongers).map(i => context.spawn(StaticPonger(), s"typed_ponger$i")).toList
            s.replyTo ! ClientRefs(getPongerPaths[StaticPing](static_pongers))
          } else {
            val pongers = (1 to s.numPongers).map(i => context.spawn(Ponger(), s"typed_ponger$i")).toList
            s.replyTo ! ClientRefs(getPongerPaths[Ping](pongers))
          }
          this
        }
        case ClientShutdown => Behaviors.stopped
      }

    }
  }

  object SystemSupervisor {
    sealed trait SystemMessage
    case class StartPingers(replyTo: ActorRef[OperationSucceeded.type], latch: CountDownLatch, numMsgs: Long, pipeline: Long, staticOnly: Boolean, pongers: ClientRefs) extends SystemMessage
    case object RunIteration extends SystemMessage
    case class StopPingers(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage
    case object GracefulShutdown extends SystemMessage
    case object OperationSucceeded

    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context))
  }

  class SystemSupervisor(context: ActorContext[SystemMessage]) extends AbstractBehavior[SystemMessage]{
    val resolver = ActorRefResolver(context.system)
    var pingers: List[ActorRef[MsgForPinger]] = null
    var static_pingers: List[ActorRef[MsgForStaticPinger]] = null
    var run_id: Int = -1
    var staticOnly = true

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case s: StartPingers => {
          this.staticOnly = s.staticOnly
          if (staticOnly){
            static_pingers = s.pongers.actorPaths.zipWithIndex.map{
              case (static_ponger, i) => context.spawn(StaticPinger(s.latch, s.numMsgs, s.pipeline, static_ponger), s"typed_staticpinger${run_id}_$i")
            }
          } else {
            pingers = s.pongers.actorPaths.zipWithIndex.map{
              case (ponger, i) => context.spawn(Pinger(s.latch, s.numMsgs, s.pipeline, ponger), s"typed_pinger${run_id}_$i")
            }
          }
          s.replyTo ! OperationSucceeded
          this
        }
        case RunIteration => {
          if (staticOnly) static_pingers.foreach(static_pinger => static_pinger ! RunStaticPinger)
          else pingers.foreach(pinger => pinger ! RunPinger)
          this
        }
        case StopPingers(replyTo) => {
          if (staticOnly){
            if (static_pingers.nonEmpty){
              static_pingers.foreach(static_pinger => context.stop(static_pinger))
              static_pingers = List.empty
            }
          } else {
            if (pingers.nonEmpty){
              pingers.foreach(pinger => context.stop(pinger))
              pingers = List.empty
            }
          }
          replyTo ! OperationSucceeded
          this
        }
        case GracefulShutdown => {
          Behaviors.stopped
        }
      }
    }
  }

  sealed trait MsgForPinger
  case class Ping(src: ActorReference, index: Long)
  case class Pong(index: Long) extends MsgForPinger
  case object RunPinger extends MsgForPinger

  object Pinger {
    def apply(latch: CountDownLatch, count: Long, pipeline: Long, ponger: String): Behavior[MsgForPinger] = Behaviors.setup(context => new Pinger(context, latch, count, pipeline, ponger))
  }

  class Pinger(context: ActorContext[MsgForPinger], latch: CountDownLatch, count: Long, pipeline: Long, pongerPath: String) extends AbstractBehavior[MsgForPinger]{
    val resolver = ActorRefResolver(context.system)
    val selfRef = ActorReference(resolver.toSerializationFormat(context.self))
    val ponger: ActorRef[Ping] = resolver.resolveActorRef(pongerPath)

    var sentCount = 0l;
    var recvCount = 0l;

    override def onMessage(msg: MsgForPinger): Behavior[MsgForPinger] = {
      msg match {
        case RunPinger => {
          var pipelined = 0l;
          while (pipelined < pipeline && sentCount < count) {
            ponger ! Ping(selfRef, sentCount)
            pipelined += 1l;
            sentCount += 1l;
          }
        }
        case Pong(_) => {
          recvCount += 1l;
          if (recvCount < count) {
            if (sentCount < count) {
              ponger ! Ping(selfRef, sentCount);
              sentCount += 1l;
            }
          } else {
            latch.countDown();
          }
        }
      }
      this
    }
  }

  object Ponger {
    def apply(): Behavior[Ping] = Behaviors.setup(context => new Ponger(context))
  }

  class Ponger(context: ActorContext[Ping]) extends AbstractBehavior[Ping]{
    val resolver = ActorRefResolver(context.system)

    private def getPingerRef(a: ActorReference): ActorRef[MsgForPinger] ={
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: Ping): Behavior[Ping] = {
      getPingerRef(msg.src) ! Pong(msg.index)
      this
    }
  }

  sealed trait MsgForStaticPinger
  case class StaticPing(src: ActorReference)
  case object StaticPong extends MsgForStaticPinger
  case object RunStaticPinger extends MsgForStaticPinger

  object StaticPinger {
    def apply(latch: CountDownLatch, count: Long, pipeline: Long, ponger: String): Behavior[MsgForStaticPinger] = Behaviors.setup(context => new StaticPinger(context, latch, count, pipeline, ponger))
  }

  class StaticPinger(context: ActorContext[MsgForStaticPinger], latch: CountDownLatch, count: Long, pipeline: Long, pongerPath: String) extends AbstractBehavior[MsgForStaticPinger]{
    val resolver = ActorRefResolver(context.system)
    val selfRef = ActorReference(resolver.toSerializationFormat(context.self))
    val ponger: ActorRef[StaticPing] = resolver.resolveActorRef(pongerPath)

    var sentCount = 0l;
    var recvCount = 0l;

    override def onMessage(msg: MsgForStaticPinger): Behavior[MsgForStaticPinger] = {
      msg match {
        case RunStaticPinger => {
          var pipelined = 0l;
          while (pipelined < pipeline && sentCount < count) {
            ponger ! StaticPing(selfRef);
            pipelined += 1l;
            sentCount += 1l;
          }
        }
        case StaticPong => {
          recvCount += 1l;
          if (recvCount < count) {
            if (sentCount < count) {
              ponger ! StaticPing(selfRef);
              sentCount += 1l;
            }
          } else {
            latch.countDown();
          }
        }
      }
      this
    }
  }

  object StaticPonger {
    def apply(): Behavior[StaticPing] = Behaviors.setup(context => new StaticPonger(context))
  }

  class StaticPonger(context: ActorContext[StaticPing]) extends AbstractBehavior[StaticPing]{
    val resolver = ActorRefResolver(context.system)

    private def getPingerRef(a: ActorReference): ActorRef[MsgForStaticPinger] ={
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: StaticPing): Behavior[StaticPing] = {
      getPingerRef(msg.src) ! StaticPong
      this
    }
  }

  object PingPongSerializer {
    val NAME = "tpnetpingpong";

    private val STATIC_PING_FLAG: Byte = 1;
    private val STATIC_PONG_FLAG: Byte = 2;
    private val PING_FLAG: Byte = 3;
    private val PONG_FLAG: Byte = 4;
  }

  class PingPongSerializer extends Serializer {
    import PingPongSerializer._
    import java.nio.{ ByteBuffer, ByteOrder }

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.TYPEDNETPPP
    override def includeManifest: Boolean = false

    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case Ping(src, index) => {
          val br = ByteString.createBuilder.putByte(PING_FLAG)
          br.putLong(index)
          val src_bytes = src.actorPath.getBytes
          br.putShort(src_bytes.size)
          br.putBytes(src_bytes)
          br.result().toArray
        }
        case StaticPing(src) => {
          val br = ByteString.createBuilder.putByte(STATIC_PING_FLAG)
          val src_bytes = src.actorPath.getBytes
          br.putShort(src_bytes.size)
          br.putBytes(src_bytes)
          br.result().toArray
        }
        case Pong(index) => ByteString.createBuilder.putByte(PONG_FLAG).putLong(index).result().toArray
        case StaticPong => Array(STATIC_PONG_FLAG)
      }
    }


    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;
      flag match {
        case PING_FLAG => {
          val index = buf.getLong
          val src_length: Int = buf.getShort
          val src_bytes = new Array[Byte](src_length)
          buf.get(src_bytes)
          val src = ActorReference(src_bytes.map(_.toChar).mkString)
          Ping(src, index)
        }
        case STATIC_PING_FLAG => {
          val src_length: Int = buf.getShort
          val src_bytes = new Array[Byte](src_length)
          buf.get(src_bytes)
          val src = ActorReference(src_bytes.map(_.toChar).mkString)
          StaticPing(src)
        }
        case PONG_FLAG => Pong(buf.getLong)
        case STATIC_PONG_FLAG => StaticPong
      }
    }
  }


}