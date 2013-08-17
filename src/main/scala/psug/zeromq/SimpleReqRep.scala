package psug.zeromq

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import org.junit.runner.RunWith
import akka.actor.{ Actor, ActorLogging, Props, actorRef2Scala }
import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec
import akka.util.ByteString
import akka.zeromq.{ Bind, Connect, Identity, Listener, SocketType, ZMQMessage, ZeroMQExtension }
import org.scalatest.junit.JUnitRunner
import akka.actor.ActorSystem
import akka.actor.PoisonPill

/**
 * Goal of the exercice:
 * "have a administrator laptop be able to send
 *  command to multiple servers and get their responses.
 *  Client can come and go, the admin laptop is here
 *  sure".
 *
 * As to not use server/service/client/etc naming, we
 * say that:
 * - Admin laptop will be called "admin".
 * - Servers will be called "nodes".
 *
 * So, we want that an admin:
 * - be able to send the same command to multiple nodes
 * - get answer, asynchonously, from each node.
 *
 * The nodes are known by advance (a node can not pop
 * from nowhere), but the list can chance for each run
 * of the program. Ideally, a node could pop from
 * nowhere and subscribes to the command-publisher
 *
 * At any time, a node can disappear. We want to have a
 * trace a such a fact.
 *
 * We can think to at least two way of handling the problem:
 *
 * 1/ admin is a client to a bunch of service (one by node)
 *
 *    We can see that as a bunch of pairs:
 *    - admin <-> node1
 *    - admin <-> node2
 *    - etc
 *
 *    That implies that when a node comes, it has some
 *    way to inform the server that it exists. It can
 *    be handled by the init script
 *
 * 2/ nodes subscribes to a command-publisher from node and
 *    send back answer to a command-result-sink in admin
 *
 *
 *
 */
object SimpleReqRepActors {
  import akka.zeromq._
  import akka.actor.Actor
  import akka.actor.Props
  import akka.actor.ActorLogging
  import akka.serialization.SerializationExtension
  import java.lang.management.ManagementFactory

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
        val repeatPayload = ser.serialize(RepeatAfterMe("Say hello!")).get

        // the first frame is the topic, second is the message
        reqSocket ! ZMQMessage(ByteString("repeat"), ByteString(repeatPayload))

      case m: ZMQMessage if m.frames(0).utf8String == "answer" =>
        val Answer(x) = ser.deserialize(m.frames(1).toArray, classOf[Answer]).get
        log.info("Got answer: " + x)

        //start a new request / response
        self ! Tick
    }
  }
}

object SimpleReqRep extends App {
  import SimpleReqRepActors._

  implicit val system = ActorSystem("zeromq")
  val socket = "tcp://127.0.0.1:30987"

  val client = system.actorOf(Props( new Requester(socket)), name = "client")
  Thread.sleep(2.seconds.toMillis)

  system.actorOf(Props( new Responder(socket)), name = "server")



  // Let it run for a while to see some output.
  // Don't do like this in real tests, this is only doc demonstration.
  Thread.sleep(2.seconds.toMillis)

  //shutdown everything
  system.shutdown
}