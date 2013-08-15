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
import akka.actor.ActorRef
import akka.actor.ActorSystem
import scala.util.Failure
import scala.util.Success
import scala.concurrent.Future
import scala.util.Try
import akka.zeromq._
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorLogging
import akka.serialization.SerializationExtension
import java.lang.management.ManagementFactory
import akka.util.Timeout
import akka.pattern.ask
import akka.pattern.pipe
import akka.pattern.gracefulStop

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



  //why, oh why Try is such a bad datatype for errors ?
  case object BadNodeProtocolAnswer extends Exception()

  case object Tick

  case class RepeatAfterMe(say: String)
  case class Answer(s:String)


  case class ExecCommand(command: String)
  case class CommandResult(result: String)

  val registrationSocket = "tcp://127.0.0.1:30987"

  implicit val timeout = Timeout(5 second)

  import scala.concurrent.ExecutionContext.Implicits.global

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
    import scala.concurrent.duration._

    //command issuer

    val askToNodes = system.actorOf(Props[AskToNodes])

    def receive = {
      case cmd:ExecCommand =>
        (askToNodes ? cmd) pipeTo sender

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

  /**
   * That one is responsible to maintain the list of node
   * to send command to (register / unregister nodes)
   * and to send command and collect response to/from them.
   */
  class AskToNodes(system: ActorSystem) extends Actor {

    //node will be identified by their socket (hostname, port)
    val nodes = collection.mutable.Map[String, ActorRef]()

    def receive = {

      //forward a command to each nodes
      //and collect responses in futures, tracking who answered
      case cmd:ExecCommand =>
        val responses = nodes.toList.map { case (id, actor) => (id, actor ? cmd)}
        sender ! responses

      //register a new node
      case RegisterNode(socket) =>
        nodes += (socket -> system.actorOf(Props(new AskToNode(socket))))

      //TODO: handle unregistration & lost connection
    }

  }


  //the command issuer is a zmq client
  //one to one connection with a node
  class AskToNode(socket: String) extends Actor with ActorLogging {
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
        //we are in ZeroMQ req/rep mode, so we need to get the answer before
        //processing an other message
        val response :  Future[Try[CommandResult]] =
          (reqSocket ? ZMQMessage(ByteString(execCmdKey), ByteString(cmdPayload))).map {
            case m: ZMQMessage if( m.frames(0).utf8String == cmdResultKey ) =>
              ser.deserialize(m.frames(1).toArray, classOf[CommandResult])

            case x                                                       =>
              Failure(BadNodeProtocolAnswer)
          }
        //send back the response to who asked
        response pipeTo sender

      //TODO: case lost connection => unregister node
    }
  }




  ///// node part /////


  /**
   * That part is the one responsible to asking for
   * registering that node in the service
   */
  class NodeRegistration(registrationServiceSocket: String, nodeSocket:String) extends Actor with ActorLogging {
    val reqSocket = ZeroMQExtension(context.system).newSocket(SocketType.Req, Listener(self),  Bind(registrationServiceSocket), Identity(nodeSocket.getBytes))
    val ser = SerializationExtension(context.system)

    override def preStart {
      val registrationPayload = ser.serialize(RegisterNode(nodeSocket)).get
      reqSocket ! ZMQMessage(ByteString(registrationQueryKey), ByteString( registrationPayload ))
    }

    override def postRestart(reason: Throwable) {
      // don't call preStart, only schedule once
    }

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if( m.frames(0).utf8String == registrationOkKey) =>
        log.info("registration OK, shutdown")
        gracefulStop(self, 5 second)
    }
  }

  //node are zmq server that execute queries
  class NodeAnswer(socket:String) extends Actor with ActorLogging {

    val repSocket = ZeroMQExtension(context.system).newSocket(SocketType.Rep, Listener(self),  Bind(socket), Identity(socket.getBytes))
    val ser = SerializationExtension(context.system)

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if( m.frames(0).utf8String == execCmdKey) =>
        val ExecCommand(cmd) = ser.deserialize(m.frames(1).toArray, classOf[ExecCommand]).get

        log.info("Got zmq message, must execute command: " + cmd)
        val repPayload = ser.serialize(Answer( s"result for command ${cmd} on ${socket}: TODO")).get
        repSocket ! ZMQMessage(ByteString("answer"), ByteString( repPayload ))

    }
  }

}

@RunWith(classOf[JUnitRunner])
class ZeromqPairsSpec extends AkkaSpec("akka.loglevel=INFO, akka.log-dead-letters-during-shutdown=false") {
  import ZeromqPairsSpec._


  "demonstrate one server and two clients" in {

    //set up two client
    //TODO


    system.actorOf(Props[UserIO], name = "querier")

    // Let it run for a while to see some output.
    // Don't do like this in real tests, this is only doc demonstration.
    Thread.sleep(2.seconds.toMillis)
  }
}