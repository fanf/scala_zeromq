package psug.aout

import language.postfixOps
import scala.concurrent.duration._
import akka.actor.{ Actor, Props }
import akka.util.ByteString
import akka.testkit._
import akka.zeromq.{ ZeroMQVersion, ZeroMQExtension, SocketType, Bind }
import java.text.SimpleDateFormat
import java.util.Date
import akka.actor.ActorRef
import akka.testkit.AkkaSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.zeromq.ZeroMQLibrary

object ZeromqDocSpec {

  //#health
  import akka.zeromq._
  import akka.actor.Actor
  import akka.actor.Props
  import akka.actor.ActorLogging
  import akka.serialization.SerializationExtension
  import java.lang.management.ManagementFactory

  case object Tick
  case class Heap(timestamp: Long, used: Long, max: Long)
  case class Load(timestamp: Long, loadAverage: Double)

  case class RepeatAfterMe(say: String)
  case class Answer(s:String)

  class HealthProbe extends Actor {

    val pubSocket = ZeroMQExtension(context.system).newSocket(SocketType.Pub, Bind("tcp://127.0.0.1:1235"))
    val memory = ManagementFactory.getMemoryMXBean
    val os = ManagementFactory.getOperatingSystemMXBean
    val ser = SerializationExtension(context.system)

    import context.dispatcher

    override def preStart() {
      context.system.scheduler.schedule(1 second, 1 second, self, Tick)
    }

    override def postRestart(reason: Throwable) {
      // don't call preStart, only schedule once
    }

    def receive: Receive = {
      case Tick =>
        val currentHeap = memory.getHeapMemoryUsage
        val timestamp = System.currentTimeMillis

        // use akka SerializationExtension to convert to bytes
        val heapPayload = ser.serialize(Heap(timestamp, currentHeap.getUsed, currentHeap.getMax)).get
        // the first frame is the topic, second is the message
        pubSocket ! ZMQMessage(ByteString("health.heap"), ByteString(heapPayload))

        // use akka SerializationExtension to convert to bytes
        val loadPayload = ser.serialize(Load(timestamp, os.getSystemLoadAverage)).get

        // the first frame is the topic, second is the message
        pubSocket ! ZMQMessage(ByteString("health.load"), ByteString(loadPayload))
    }
  }
  //#health

  //#logger
  class Logger extends Actor with ActorLogging {

    ZeroMQExtension(context.system).newSocket(SocketType.Sub, Listener(self), Connect("tcp://127.0.0.1:1235"), Subscribe("health"))

    val ser = SerializationExtension(context.system)
    val timestampFormat = new SimpleDateFormat("HH:mm:ss.SSS")

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if m.frames(0).utf8String == "health.heap" =>
        val Heap(timestamp, used, max) = ser.deserialize(m.frames(1).toArray, classOf[Heap]).get
        log.info("Used heap {} bytes, at {}", used, timestampFormat.format(new Date(timestamp)))

      case m: ZMQMessage if m.frames(0).utf8String == "health.load" =>
        val Load(timestamp, loadAverage) = ser.deserialize(m.frames(1).toArray, classOf[Load]).get
        log.info("Load average {}, at {}", loadAverage, timestampFormat.format(new Date(timestamp)))
    }
  }
  //#logger

  //#alerter
  class HeapAlerter extends Actor with ActorLogging {

    ZeroMQExtension(context.system).newSocket(SocketType.Sub, Listener(self), Connect("tcp://127.0.0.1:1235"), Subscribe("health.heap"))
    val ser = SerializationExtension(context.system)
    var count = 0

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if m.frames(0).utf8String == "health.heap" =>
        val Heap(timestamp, used, max) = ser.deserialize(m.frames(1).toArray, classOf[Heap]).get

        if ((used.toDouble / max) > 0.9) count += 1
        else count = 0

        if (count > 10) log.warning("Need more memory, using {} %", (100.0 * used / max))
    }
  }
  //#alerter





  val socket = "tcp://127.0.0.1:30987"

  //the command issuer is a zmq client
  class CommandIssuer extends Actor with ActorLogging {

    //self, i.e CommandIssuer, listen to message gotten by repSocket
    //needed to get back answer from service
    val reqSocket = ZeroMQExtension(context.system).newSocket(SocketType.Req, Listener(self), Connect(socket), Identity("cmdIssuer".getBytes))
    val ser = SerializationExtension(context.system)

    import context.dispatcher

    override def preStart() {
      self ! Tick
//      context.system.scheduler.schedule(1 second, 1 second, self, Tick)
    }

    override def postRestart(reason: Throwable) {
      // don't call preStart, only schedule once
    }

    def receive: Receive = {
      case Tick =>
        log.info("At least I'm starting!")

        //ask each client to repeat after him
        val timestamp = System.currentTimeMillis

        // use akka SerializationExtension to convert to bytes
        val repeatPayload = ser.serialize(RepeatAfterMe("now, it's " + timestamp.toString)).get

        // the first frame is the topic, second is the message
        reqSocket ! ZMQMessage(ByteString("repeat"), ByteString(repeatPayload))

      case m: ZMQMessage if m.frames(0).utf8String == "answer" =>
        val Answer(x) = ser.deserialize(m.frames(1).toArray, classOf[Answer]).get
        log.info("Got answer: " + x)

        self ! Tick

    }
  }

  //node are zmq server
  trait CommandNode extends Actor with ActorLogging {
    def name: String

    val repSocket = ZeroMQExtension(context.system).newSocket(SocketType.Rep, Listener(self),  Bind(socket), Identity(name.getBytes))
    val ser = SerializationExtension(context.system)

    log.info("At least I'm starting!")

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if m.frames(0).utf8String == "repeat" =>
        val RepeatAfterMe(timestamp) = ser.deserialize(m.frames(1).toArray, classOf[RepeatAfterMe]).get

        log.info("Got zmq message, must repeat: " + timestamp)
        val repPayload = ser.serialize(Answer( s"I'm ${name} and I repeat ${timestamp}")).get
        repSocket ! ZMQMessage(ByteString("answer"), ByteString( repPayload ))

    }
  }

  class CommandNode1 extends CommandNode { def name = "cmd_1" }

}

@RunWith(classOf[JUnitRunner])
class ZeromqDocSpec extends AkkaSpec("akka.loglevel=INFO, akka.log-dead-letters-during-shutdown=false") {
  import ZeromqDocSpec._


  "demonstrate one server and two clients" in {
    checkZeroMQInstallation()

    system.actorOf(Props[CommandNode1], name = "node1")

    system.actorOf(Props[CommandIssuer], name = "querier")
    // Let it run for a while to see some output.
    // Don't do like this in real tests, this is only doc demonstration.
    Thread.sleep(10.seconds.toMillis)
  }

//  "demonstrate how to create socket" in {
//    checkZeroMQInstallation()
//
//    //#pub-socket
//    import akka.zeromq.ZeroMQExtension
//
//    val pubSocket = ZeroMQExtension(system).newSocket(SocketType.Pub, Bind("tcp://127.0.0.1:21231"))
//    //#pub-socket
//
//    import akka.zeromq._
//    val sub: { def subSocket: ActorRef; def listener: ActorRef } = new AnyRef {
//      //#sub-socket
//      import akka.zeromq._
//
//      class Listener extends Actor {
//        def receive: Receive = {
//          case Connecting ⇒ //...
//          case m: ZMQMessage ⇒ //...
//          case _ ⇒ //...
//        }
//      }
//
//      val listener = system.actorOf(Props(classOf[Listener], this))
//      val subSocket = ZeroMQExtension(system).newSocket(
//          SocketType.Sub
//        , Listener(listener)
//        , Connect("tcp://127.0.0.1:21231")
//        , SubscribeAll
//      )
//      //#sub-socket
//    }
//    val listener = sub.listener
//
//    //#sub-topic-socket
//    val subTopicSocket = ZeroMQExtension(system).newSocket(
//        SocketType.Sub
//      , Listener(listener)
//      , Connect("tcp://127.0.0.1:21231")
//      , Subscribe("foo.bar")
//    )
//    //#sub-topic-socket
//
//    //#unsub-topic-socket
//    subTopicSocket ! Unsubscribe("foo.bar")
//    //#unsub-topic-socket
//
//    val payload = Array.empty[Byte]
//    //#pub-topic
//    pubSocket ! ZMQMessage(ByteString("foo.bar"), ByteString(payload))
//    //#pub-topic
//
//    system.stop(sub.subSocket)
//    system.stop(subTopicSocket)
//
//    //#high-watermark
//    val highWatermarkSocket = ZeroMQExtension(system).newSocket(
//      SocketType.Router,
//      Listener(listener),
//      Bind("tcp://127.0.0.1:21233"),
//      HighWatermark(50000))
//    //#high-watermark
//  }

//  "demonstrate pub-sub" in {
//    checkZeroMQInstallation()
//
//    //#health
//
//    system.actorOf(Props[HealthProbe], name = "health")
//    //#health
//
//    //#logger
//
//    system.actorOf(Props[Logger], name = "logger")
//    //#logger
//
//    //#alerter
//
//    system.actorOf(Props[HeapAlerter], name = "alerter")
//    //#alerter
//
//    // Let it run for a while to see some output.
//    // Don't do like this in real tests, this is only doc demonstration.
//    Thread.sleep(10.seconds.toMillis)
//
//  }

  def checkZeroMQInstallation() = try {
    ZeroMQExtension(system).version match {
      case ZeroMQVersion(2, x, _) if x >= 1 ⇒ Unit
      case ZeroMQVersion(y, _, _) if y >= 3 ⇒ Unit
      case version ⇒
        println("linkage error: bad version: " + version)
        pending
    }
  } catch {
    case e: LinkageError ⇒
      println("linkage error, exception: " + e.getMessage())
      e.printStackTrace()
      pending
  }
}