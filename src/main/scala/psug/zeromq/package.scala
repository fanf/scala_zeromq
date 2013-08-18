package psug

import java.net.ServerSocket

package object zeromq {

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