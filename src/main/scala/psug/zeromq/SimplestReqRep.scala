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
object SimplestReqRep extends App {
  //Test actor that prints all ZEROMQ message it gets

  def msg(s:String) = ZMQMessage(ByteString(s))

  class Listener extends Actor with ActorLogging {
    def receive = {
      case m: ZMQMessage => log.info("Got message: " + m.frames(0).utf8String)
      case Connecting => log.info("Got a connection from " + sender.path)
      case Closed => log.info("Lost connection to " + sender.path)
    }
  }

  implicit val system = ActorSystem("zeromq")
  val socket = "tcp://127.0.0.1:5000"

  val listener = system.actorOf(Props[Listener], "listener")

  val server = ZeroMQExtension(system).newSocket(
      SocketType.Rep
    , Listener(listener) // listener will get message from ZeroMQ socket
    , Bind(socket)   // a service binds a socket and wait for connection
  )

  val client1 = ZeroMQExtension(system).newSocket(
      SocketType.Req
    , Listener(listener)
    , Connect(socket)
  )

  val client2 = ZeroMQExtension(system).newSocket(
      SocketType.Req
    , Listener(listener)
    , Connect(socket)
  )

  client1 ! msg("hello from client1")
  Thread.sleep(1.seconds.toMillis)

  server ! msg("hi")
  Thread.sleep(1.seconds.toMillis)

  client1 ! msg("howdy?")
  Thread.sleep(1.seconds.toMillis)

  server ! msg("OK, thanks you")
  Thread.sleep(1.seconds.toMillis)

  client2 ! msg("Wow, what's up server")
  Thread.sleep(1.seconds.toMillis)

  server ! msg("talking to client1")
  Thread.sleep(1.seconds.toMillis)

  client2 ! msg("You don't talk to other! /me rage quit")
  system.stop(client2)
  Thread.sleep(1.seconds.toMillis)


  server ! msg("ho, client2...")
  Thread.sleep(1.seconds.toMillis)


  println("Stopping everything")
  //shutdown everything
  system.shutdown
}