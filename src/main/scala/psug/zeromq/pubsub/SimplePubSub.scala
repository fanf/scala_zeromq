package psug.zeromq
package pubsub

import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{ Actor, ActorLogging, ActorSystem, Props, actorRef2Scala }
import akka.zeromq.{ Bind, Closed, Connect, Connecting, Listener, SocketType, ZMQMessage, ZeroMQExtension }
import akka.zeromq.Subscribe
import akka.actor.PoisonPill
import akka.util.ByteString

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
object SimplePubSubActors {
  case object Tick

  //a zmq server
  class Server(socket:String) extends Actor with ActorLogging {

    val pubSocket = ZeroMQExtension(context.system).newSocket(
        SocketType.Pub
      , Bind(socket)
    )

    override def preStart() {
      log.info("Starting server!")
      self ! Tick
    }

    var i = 0

    def receive: Receive = {
      case Tick =>
        // the first frame is the topic, second is the message
        val msg = s"${self.path.name} say: ${i}"
        pubSocket ! ZMQMessage( ByteString((i%2).toString), ByteString(msg) )
        i += 1
        context.system.scheduler.scheduleOnce(200 milliseconds, self, Tick)

    }
  }

  class Client(socket:String, subscribe0 : Boolean = false, subscribe1: Boolean = false) extends Actor with ActorLogging {

    val socketOptions = Seq(
        Some(SocketType.Sub)
      , Some(Listener(self)) // Responder will get message from ZeroMQ socket
      , Some(Connect(socket))   // a service binds a socket and wait for connection
      , Some(Subscribe("0")).filter(_ => subscribe0)
      , Some(Subscribe("1")).filter(_ => subscribe1)
    ).flatten

    val subSocket = ZeroMQExtension(context.system).newSocket(
        socketOptions:_*
    )


    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage =>
        log.info(m.frames(1).utf8String)

      case Connecting => log.info("Connection from " + sender.path)
    }
  }

}

object SimplePubSub extends App {
  import SimplePubSubActors._

  implicit val system = ActorSystem("zeromq")
  val socket = "tcp://127.0.0.1:" + nextPort
  println("socket: " + socket)

  val client = system.actorOf(Props( new Client(socket, true, true)), name = "client_1")

  system.actorOf(Props( new Server(socket)), name = "server")

  system.actorOf(Props( new Client(socket, true, true)), name = "client_2")

  Thread.sleep(20.seconds.toMillis)

  println("Stopping client 1")

  //stop client 1
  client ! PoisonPill

  Thread.sleep(1.seconds.toMillis)

  forceShutdown
}