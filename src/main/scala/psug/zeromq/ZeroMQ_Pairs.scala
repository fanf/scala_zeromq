package psug.zeromq

import java.net.ServerSocket
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{ Success, Failure, Try }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props, actorRef2Scala }
import akka.pattern.{ ask, pipe }
import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec
import akka.util.{ ByteString, Timeout }
import akka.zeromq._
import akka.actor.PoisonPill
import com.typesafe.config.ConfigFactory

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


  implicit val timeout = Timeout(2 second)


  //socket should looks like: tcp://127.0.0.1:30987
  final case class RegisterNode(name: String, socket: String)

  val registrationQueryKey = "register"
  val registrationOkKey = "registration OK"
  val execCmdKey = "execCmd"
  val cmdResultKey = "cmdResul"


  //val newSocket = new ServerSocket(0)
  //println("listening on port: " + s.getLocalPort)


  //The main actor: wait for user command, send them to a command manager,
  //send back response to the user
  class Admin(registrationSocket: String) extends Actor with ActorLogging {
    import scala.concurrent.Await
    import akka.pattern.ask
    import scala.concurrent.duration._

    //command issuer
    val askNodes = context.actorOf( Props[AskNodes], name = "askNodes" )

    //registration service
    val registrationService = context.actorOf(Props(new RegistrationService(registrationSocket, askNodes)), name = "registrationService")

    def receive = {
      case cmd:ExecCommand =>
        (askNodes ? cmd) pipeTo sender

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

  class RegistrationService(registrationSocket: String, commandSender: ActorRef) extends Actor with ActorLogging {

    val repSocket = ZeroMQExtension(context.system).newSocket(SocketType.Rep, Listener(self),  Bind(registrationSocket), Identity("registrationService".getBytes))
    val ser = SerializationExtension(context.system)

    log.info("Registration service up at socket " + registrationSocket)

    //only answer to "connect to" command by spawning new actors
    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if m.frames(0).utf8String == registrationQueryKey =>
        val RegisterNode(name, socket) = ser.deserialize(m.frames(1).toArray, classOf[RegisterNode]).get
        log.info(s"Registering new node ${name} with socket ${socket}")

        //send a message to create the actor for connection to node
        commandSender ! RegisterNode(name, socket)

        //answer that connection is OK so that the node can close its registration socket.
        repSocket ! ZMQMessage(ByteString(registrationOkKey))

      case Connecting =>
        log.error("New connection establish for registration by " + sender.path)

      case Closed =>
        log.error("Connection lost/closed")
    }
  }

  /**
   * That one is responsible to maintain the list of node
   * to send command to (register / unregister nodes)
   * and to send command and collect response to/from them.
   */
  class AskNodes() extends Actor {

    //node will be identified by their socket (hostname, port)
    val nodes = collection.mutable.Map[String, ActorRef]()

    def receive = {

      //forward a command to each nodes
      //and collect responses in futures, tracking who answered
      case cmd:ExecCommand =>
        val responses = nodes.toList.map { case (id, actor) => (id, actor ? cmd)}
        sender ! responses

      //register a new node
      case RegisterNode(name, socket) =>
        nodes += (socket -> context.actorOf(Props(new AskToNode(socket)), name = name))

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

      case Connecting =>
        log.error("New connection establish for registration by " + sender.path)

      case Closed =>
        log.error("Connection lost/closed")

    }
  }



  //////////////////////////////////////////////
  //////////////////// node ////////////////////
  //////////////////////////////////////////////


  class Node(registrationServiceSocket: String, nodeSocket:String) extends Actor with ActorLogging {

    //start to listen for command
    val answer = context.actorOf(Props(new NodeAnswer(nodeSocket)), name = "answer")

    // a dismissable actor for registration
    val register = context.actorOf(Props(new NodeRegistration(registrationServiceSocket, nodeSocket)), "registerer")

    def receive: Receive = {

      case PoisonPill =>
        context.stop(register)
        context.stop(answer)
        context.stop(self)
    }

  }


  /**
   * That part is the one responsible to asking for
   * registering that node in the service
   */
  class NodeRegistration(registrationServiceSocket: String, nodeSocket:String) extends Actor with ActorLogging {
    val reqSocket = ZeroMQExtension(context.system).newSocket(SocketType.Req, Listener(self), Connect(registrationServiceSocket), Identity(nodeSocket.getBytes))
    val ser = SerializationExtension(context.system)

    override def preStart {
      val nodeName = self.path.parent.name
      val registrationPayload = ser.serialize(RegisterNode(nodeName, nodeSocket)).get
      log.info(s"registering node ${nodeName} with socket ${nodeSocket} on registration service ${registrationServiceSocket}")
      reqSocket ! ZMQMessage(ByteString(registrationQueryKey), ByteString( registrationPayload ))
    }

    override def postRestart(reason: Throwable) {
      // don't call preStart, only schedule once
    }

    def receive = {
      // the first frame is the topic, second is the message
      case m: ZMQMessage if( m.frames(0).utf8String == registrationOkKey) =>
        log.info("registration OK, shutdown")
        context.stop(reqSocket)
        context.stop(self)

      case Connecting =>
        log.error("New connection establish for registration by " + sender.path)

      case Closed =>
        log.error("Connection lost/closed")
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


      case Connecting =>
        log.debug("New connection establish for asking to exec command by " + sender.path)

      case Closed =>
        log.error("Connection lost/closed")
    }
  }

}

object ZeromqPairs extends App {


  import ZeromqPairsSpec._

  def nextSocket() = new ServerSocket(0).getLocalPort

  val registrationSocket = "tcp://127.0.0.1:30987"
  val node1Socket = s"tcp://127.0.0.1:${nextSocket}"
  val node2Socket = s"tcp://127.0.0.1:${nextSocket}"

  implicit val system = ActorSystem("zeromq")



  val admin = system.actorOf(Props(new Admin(registrationSocket)), name = "admin")

  def exec(cmd:String) : Unit = (admin ? ExecCommand(cmd)).onComplete {
    case Success(results:List[_]) =>
      results.foreach { case (id, res) => res match {
        case future:Future[Try[CommandResult]] => future.onComplete {
          case Success(Success(CommandResult(res))) =>  println("command result: " + res)
          case Success(x) => println("unexpected: " + x)
          case Failure(x) => println("fails: " + x)
        }
        case x => println("unexpected unexpected: " + x)
      } }
    case Failure(x) => println("fails: " + x)
  }



  //first node
  val node1 = system.actorOf(Props(new Node(registrationSocket, node1Socket)), name = "node1")

  Thread.sleep(2.seconds.toMillis)

  //issue a command

  exec("ls cmd")

  //start an other node

//    val node2 = system.actorOf(Props(new Node(registrationSocket, node2Socket)), name = "node2")
//
//    Thread.sleep(1.seconds.toMillis)
//
//    //issue an other command
//    exec("rm -rf /")


  // Let it run for a while to see some output.
  // Don't do like this in real tests, this is only doc demonstration.
  Thread.sleep(5.seconds.toMillis)
  system.shutdown
}