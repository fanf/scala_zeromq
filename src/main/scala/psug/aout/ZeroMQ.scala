package psug.aout

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import org.junit.runner.RunWith
import akka.actor.{ Actor, ActorLogging, Props, actorRef2Scala }
import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec
import akka.util.ByteString
import akka.zeromq.{ Bind, Connect, Identity, Listener, SocketType, ZMQMessage, ZeroMQExtension }
import org.scalatest.junit.JUnitRunner

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
object ZeromqDocSpec {





  import akka.zeromq._
  import akka.actor.Actor
  import akka.actor.Props
  import akka.actor.ActorLogging
  import akka.serialization.SerializationExtension
  import java.lang.management.ManagementFactory

  case object Tick

  case class RepeatAfterMe(say: String)
  case class Answer(s:String)


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

  class CommandNode2 extends CommandNode { def name = "cmd_2" }
}

@RunWith(classOf[JUnitRunner])
class ZeromqDocSpec extends AkkaSpec("akka.loglevel=INFO, akka.log-dead-letters-during-shutdown=false") {
  import ZeromqDocSpec._


  "demonstrate one server and two clients" in {

    system.actorOf(Props[CommandNode1], name = "node1")
    system.actorOf(Props[CommandNode2], name = "node2")

    system.actorOf(Props[CommandIssuer], name = "querier")

    // Let it run for a while to see some output.
    // Don't do like this in real tests, this is only doc demonstration.
    Thread.sleep(2.seconds.toMillis)
  }
}