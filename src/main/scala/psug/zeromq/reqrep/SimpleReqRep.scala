package psug.zeromq
package reqrep

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

import akka.actor.{ Actor, ActorLogging, ActorSystem, PoisonPill, Props, actorRef2Scala }
import akka.zeromq.{ Bind, Connect, Connecting, Listener, SocketType, ZMQMessage, ZeroMQExtension }

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

    override def preStart() {
      log.info("Starting client: " + self.path.name)
      self ! Tick
    }

    def receive: Receive = {
      case Tick =>
        // the first frame is the topic, second is the message
        reqSocket ! zmqMsg(s"Say hello from ${self.path.name}")

      case m: ZMQMessage =>
        log.info("Got answer: " + m.frames(0).utf8String)
        context.system.scheduler.scheduleOnce(100 milliseconds, self, Tick)

      case Connecting => log.info("Connection from " + sender.path)
    }
  }
}

object SimpleReqRep extends App {
  import SimpleReqRepActors._

  implicit val system = ActorSystem("zeromq")
  val socket = "tcp://127.0.0.1:" + nextPort
  println("socket: " + socket)

  val client = system.actorOf(Props( new Requester(socket)), name = "client_1")

  system.actorOf(Props( new Responder(socket)), name = "server")

  system.actorOf(Props( new Requester(socket)), name = "client_2")

  Thread.sleep(2.seconds.toMillis)

  println("Stopping client 1")

  //stop client 1
  client ! PoisonPill

  Thread.sleep(1.seconds.toMillis)

  forceShutdown
}