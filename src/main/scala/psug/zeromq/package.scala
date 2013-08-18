package psug

import java.net.ServerSocket
import akka.util.ByteString
import akka.zeromq.ZMQMessage
import akka.actor.ActorSystem

package object zeromq {


  def zmqMsg(s: String*) : ZMQMessage = ZMQMessage(s.toSeq.map(ByteString(_)):_*)

  def forceShutdown(implicit system: ActorSystem) : Unit = {
    println("Stopping everything")
    //shutdown everything
    system.shutdown
    Thread.sleep(500)
    System.exit(0)
  }

  /**
   * Get the next available port.
   * Java... Oh why Java...
   */
  def nextPort : Int = {
    val s = new ServerSocket(0)
    try {
      s.getLocalPort
    } finally {
      s.close
    }
  }

}