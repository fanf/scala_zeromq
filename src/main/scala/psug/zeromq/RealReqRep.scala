package psug.zeromq

import scala.concurrent.duration.DurationInt
import akka.actor.{ Actor, ActorLogging, ActorSystem, PoisonPill, Props, actorRef2Scala }
import akka.serialization.SerializationExtension
import akka.util.ByteString
import akka.zeromq.{ Bind, Connect, Listener, SocketType, ZMQMessage, ZeroMQExtension }
import java.util.zip.ZStreamRef
import akka.actor.ActorRef
import akka.pattern.ask
import scala.util.Failure
import scala.util.Success

/**
 * Goal of the exercice:
 *
 *  Add to simple rep/req the "ask" semantics
 *
 */
object RealReqRepActors {
  import akka.zeromq._
  import akka.actor.Actor
  import akka.actor.Props
  import akka.actor.ActorLogging
  import akka.serialization.SerializationExtension
  import java.lang.management.ManagementFactory
  import akka.pattern.ask
  import scala.concurrent.ExecutionContext.Implicits.global

  case object Tick

  case class RepeatAfterMe(say: String)
  case class Answer(s:String)

  case object Plop

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

        log.info("Server was asked to repeat: " + repeat)
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

    var responseCollector: ActorRef = null

    def receive: Receive = {
      case Tick =>

        responseCollector = sender
        // use akka SerializationExtension to convert to bytes
        val repeatPayload = ser.serialize(RepeatAfterMe("Say hello!")).get

        // the first frame is the topic, second is the message
        reqSocket ! ZMQMessage(ByteString("repeat"), ByteString(repeatPayload))

      case m: ZMQMessage if m.frames(0).utf8String == "answer" =>
        val Answer(x) = ser.deserialize(m.frames(1).toArray, classOf[Answer]).get
        log.info("client got response, forwarding to collector")
        responseCollector ! Answer(x)
        responseCollector = null
    }
  }

  class MessageCollector(socket: String) extends Actor with ActorLogging {

    val client = context.actorOf(Props(new Requester(socket)), name = "client")

    def receive: Receive = {
      case Tick =>
        ask(client, Tick)(5 second).onComplete {
          case Failure(x) => log.error(x.getMessage)
          case Success(x) => log.info("Collector got message: " + x)
        }
    }
  }

}

object RealReqRep extends App {
  import RealReqRepActors._

  implicit val system = ActorSystem("zeromq")
  val socket = "tcp://127.0.0.1:30987"

  val messageCollector = system.actorOf(Props( new MessageCollector(socket)), name = "client")

  messageCollector ! Tick

  Thread.sleep(2.seconds.toMillis)

  system.actorOf(Props( new Responder(socket)), name = "server")


  Thread.sleep(2.seconds.toMillis)
  println("Stopping everything")
  //shutdown everything
  system.shutdown
}