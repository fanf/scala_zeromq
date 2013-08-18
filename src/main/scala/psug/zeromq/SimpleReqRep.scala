package psug.zeromq

import java.net.ServerSocket

import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor.{ Actor, ActorLogging, ActorSystem, PoisonPill, Props, actorRef2Scala }
import akka.serialization.SerializationExtension
import akka.util.ByteString
import akka.zeromq._

/**
 * Goal of the exercice:
 *
 * Demonstrate how simple Request / Response works
 * in ZeroMQ / AKKA.
 *
 * In particular, we are going to see how multiple client
 * can connect to a server.
 *
 */
object SimpleReqRepActors {

  case object Tick

  case class RepeatAfterMe(say: String)
  case class Answer(s:String)

  //a zmq server
  class Responder(socket:String) extends Actor with ActorLogging {

    val repSocket = ZeroMQExtension(context.system).newSocket(
        SocketType.Rep
      , Listener(self) // Responder will get message from ZeroMQ socket
      , Bind(socket)   // a service binds a socket and wait for connection
    )
    val ser = SerializationExtension(context.system)

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if m.frames(0).utf8String == "repeat" =>
        val RepeatAfterMe(repeat) = ser.deserialize(m.frames(1).toArray, classOf[RepeatAfterMe]).get

        val repPayload = ser.serialize(Answer( s"I repeat ${repeat}")).get
        repSocket ! ZMQMessage(ByteString("answer"), ByteString( repPayload ))
    }
  }

  //socket that issue requests
  class Requester(socket:String) extends Actor with ActorLogging {

    //self, i.e CommandIssuer, listen to message gotten by repSocket
    //needed to get back answer from service
    val reqSocket = ZeroMQExtension(context.system).newSocket(
        SocketType.Req
      , Listener(self)
      , Connect(socket)
    )
    val ser = SerializationExtension(context.system)


    override def preStart() {
      self ! Tick
    }

    def receive: Receive = {
      case Tick =>

        // use akka SerializationExtension to convert to bytes
        val repeatPayload = ser.serialize(RepeatAfterMe("Say hello from " + self.path.name)).get

        // the first frame is the topic, second is the message
        reqSocket ! ZMQMessage(ByteString("repeat"), ByteString(repeatPayload))

      case m: ZMQMessage if m.frames(0).utf8String == "answer" =>
        val Answer(x) = ser.deserialize(m.frames(1).toArray, classOf[Answer]).get
        log.info("Got answer: " + x)

        context.system.scheduler.scheduleOnce(1 seconds, self, Tick)
    }
  }
}

object SimpleReqRep extends App {
  import SimpleReqRepActors._

  implicit val system = ActorSystem("zeromq")
  val socket = "tcp://127.0.0.1:" + nextPort
  println("socket: " + socket)

  val client = system.actorOf(Props( new Requester(socket)), name = "client_1")
  Thread.sleep(2.seconds.toMillis)

  system.actorOf(Props( new Responder(socket)), name = "server")

  Thread.sleep(2.seconds.toMillis)

  system.actorOf(Props( new Requester(socket)), name = "client_2")


  // Let it run for a while to see some output.
  // Don't do like this in real tests, this is only doc demonstration.
  Thread.sleep(2.seconds.toMillis)

  println("Stopping client 1")

  //stop the client
  client ! PoisonPill

  Thread.sleep(1.seconds.toMillis)

  println("Stopping everything")
  //shutdown everything
  system.shutdown
}