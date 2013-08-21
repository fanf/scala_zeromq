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
 *
 * Integration of ZeroMQ / AKKA with PUB/SUB
 *
 */
object SimplePubSubActors {
  case object Tick

  //a zmq publisher actor
  class Server(socket:String) extends Actor with ActorLogging {

    //This is how are created new ZMQ sockets with AKKA
    //It is itself an actor
    val pubSocket = ZeroMQExtension(context.system).newSocket(
        SocketType.Pub //publisher socket
      , Bind(socket) //
    )

    override def preStart() {
      log.info("Starting server!")
      self ! Tick
    }

    var i = 0

    def receive: Receive = {
      case Tick =>
        val msg = s"${self.path.name} say: ${i}"
        // Publish a message.
        // The first frame is the topic, second is the message.
        // Never EVER forget ZMQMessage, else nothing will happen
        pubSocket ! ZMQMessage( ByteString((i%2).toString), ByteString(msg) )
        i += 1
        context.system.scheduler.scheduleOnce(200 milliseconds, self, Tick)

    }
  }

  //the client actor
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
  val socket = "tcp://*:5000"

  val client = system.actorOf(Props( new Client(socket, true, true)), name = "client_1")

  //server can be created after client
  system.actorOf(Props( new Server(socket)), name = "server")

  //a second actor
  system.actorOf(Props( new Client(socket, true, true)), name = "client_2")

  Thread.sleep(20.seconds.toMillis)

  println("Stopping client 1")

  //stop client 1
  client ! PoisonPill

  Thread.sleep(1.seconds.toMillis)

//  forceShutdown
}