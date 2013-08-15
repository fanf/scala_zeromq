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
import scala.actors.ActorRef
import akka.actor.ActorSystem

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
object ZeromqPairsSpec {

  import akka.zeromq._
  import akka.actor.Actor
  import akka.actor.Props
  import akka.actor.ActorLogging
  import akka.serialization.SerializationExtension
  import java.lang.management.ManagementFactory

  case object Tick

  case class RepeatAfterMe(say: String)
  case class Answer(s:String)


  case class ExecCommand(command: String)
  case class CommandResult(result: String)
  case class NodeCommandResult(who: String, result: String)


  val registrationSocket = "tcp://127.0.0.1:30987"

  //socket should looks like: tcp://127.0.0.1:30987
  final case class RegisterNode(socket: String)

  val registrationQueryKey = "register"
  val registrationOkKey = "registration OK"
  val execCmdKey = "execCmd"
  val cmdResultKey = "cmdResul"

  //val newSocket = new ServerSocket(0)
  //println("listening on port: " + s.getLocalPort)


  //The main actor: wait for user command, send them to a command manager,
  //send back response to the user
  class UserIO(system: ActorSystem) extends Actor with ActorLogging {
    import scala.concurrent.Await
    import akka.pattern.ask
    import akka.util.Timeout
    import scala.concurrent.duration._
    import akka.pattern.pipe

    import scala.concurrent.ExecutionContext.Implicits.global

    implicit val timeout = Timeout(5 second)

    //command issuer

    val nodeCommandManager = system.actorOf(Props[CommandManager])

    def receive = {
      case cmd:ExecCommand =>
        (nodeCommandManager ? cmd) pipeTo sender

    }

  }

  /*
   * We have 4 types of actor for ZMQ management:
   * - admin has a Registration Rep, always waiting for new servers
   * - node has a registration Req, that only exchange one time
   * - after registration, a couple of rep/req is created:
   *   - on admin, a req that will issue command
   *   - on node, a rep that will respond to command received
   *
   * Then, we have a master actor which interface with the user.
   */

  class registrationService(commandSender: ActorRef) extends Actor with ActorLogging {

    val repSocket = ZeroMQExtension(context.system).newSocket(SocketType.Rep, Listener(self),  Bind(registrationSocket), Identity("registrationService".getBytes))
    val ser = SerializationExtension(context.system)

    //only answer to "connect to" command by spawning new actors
    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if m.frames(0).utf8String == registrationQueryKey =>
        val RegisterNode(socket) = ser.deserialize(m.frames(1).toArray, classOf[RegisterNode]).get

        //send a message to create the actor for connection to node
        commandSender ! RegisterNode(socket)

        //answer that connection is OK so that the node can close its registration socket.
        repSocket ! ZMQMessage(ByteString(registrationOkKey))

    }
  }




  //the command issuer is a zmq client
  //one to one connection with a node
  class CommandIssuer(socket: String, commandManager: ActorRef) extends Actor with ActorLogging {

    //self, i.e CommandIssuer, listen to message gotten by repSocket
    //needed to get back answer from service
    val reqSocket = ZeroMQExtension(context.system).newSocket(SocketType.Req, Listener(self), Connect(socket), Identity(socket.getBytes))
    val ser = SerializationExtension(context.system)

    def receive: Receive = {

      //user ask for a new command
      case ExecCommand(cmd) =>
        //ask the node to execute the command
        val cmdPayload = ser.serialize(ExecCommand(cmd)).get

        // the first frame is the topic, second is the message
        reqSocket ! ZMQMessage(ByteString(execCmdKey), ByteString(cmdPayload))

      //got an answer from a node
      case m: ZMQMessage if m.frames(0).utf8String == cmdResultKey =>
        val CommandResult(result) = ser.deserialize(m.frames(1).toArray, classOf[CommandResult]).get

        //forward to master of command
        commandManager ! NodeCommandResult(socket, result)
    }
  }


  class CommandManager extends Actor {

  }



  //node are zmq server that execute queries
  class CommandNode(socket:String) extends Actor with ActorLogging {

    val repSocket = ZeroMQExtension(context.system).newSocket(SocketType.Rep, Listener(self),  Bind(socket), Identity(socket.getBytes))
    val ser = SerializationExtension(context.system)

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if m.frames(0).utf8String == "repeat" =>
        val ExecCommand(cmd) = ser.deserialize(m.frames(1).toArray, classOf[execCmdKey]).get

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