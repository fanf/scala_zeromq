package psug.zeromq

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Success }
import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props, actorRef2Scala }
import akka.pattern.ask
import akka.util.Timeout.durationToTimeout
import akka.zeromq.{ Bind, Connect, Listener, SocketType, ZMQMessage, ZeroMQExtension }
import akka.util.ByteString
import scala.util.Success

/**
 * Goal of the exercice:
 *
 *  Add to simple rep/req the "ask" semantics
 *
 */
object RealReqRepActors {

  case object Tick

  /**
   * We want to use the comman "ask" idiom, and don't
   * know that underlining, we use ZeroMQ
   */
  class MessageCollector(socket: String) extends Actor with ActorLogging {

    val client = context.actorOf(Props(new Requester(socket)), name = "client")

    def receive: Receive = {
      case Tick =>
        ask(client, Tick)(5 second).onComplete {
          case Failure(x) => log.error(x.getMessage)
          case Success(ZMQMessage(x:Seq[ByteString])) => log.info("Collector got message: " + x(0).utf8String)
          case Success(x) => log.error("Haven expected that: " + x)
        }
    }
  }

  //a zmq server
  class Responder(socket:String) extends Actor with ActorLogging {

    val repSocket = ZeroMQExtension(context.system).newSocket(
        SocketType.Rep
      , Listener(self) // Responder will get message from ZeroMQ socket
      , Bind(socket)   // a service binds a socket and wait for connection
    )

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage =>
        val repeat = m.frames(0).utf8String

        log.info("Server was asked to repeat: " + repeat)
        repSocket ! zmqMsg(s"I repeat ${repeat}")
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

    var responseCollector: ActorRef = null

    override def preStart() = {
      log.info("Starting requester: " + self.path.name)
    }

    def receive: Receive = {
      case Tick =>

        responseCollector = sender
        reqSocket ! zmqMsg("Say hello!")

      case m: ZMQMessage =>
        val x  = m.frames(0).utf8String
        log.info("client got response, forwarding to collector")
        responseCollector ! zmqMsg(x)
        responseCollector = null
    }
  }



}

object RealReqRep extends App {
  import RealReqRepActors._

  implicit val system = ActorSystem("zeromq")
  val socket = "tcp://127.0.0.1:" + nextPort

  val messageCollector = system.actorOf(Props( new MessageCollector(socket)), name = "client")

  messageCollector ! Tick

  Thread.sleep(2.seconds.toMillis)

  system.actorOf(Props( new Responder(socket)), name = "server")


  Thread.sleep(2.seconds.toMillis)

  forceShutdown
}