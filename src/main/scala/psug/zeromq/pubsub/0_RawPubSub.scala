package psug.zeromq
package pubsub

import org.zeromq.ZMQ

/**
 * Simple PUB/SUB using directly libzmq JNA bindings
 */


/**
 * The PUBLISH server
 */
object RawPubSubServer extends App {

  //define the libzmq context to use. the 1 mean
  //"one I/O thread", add one for each GB/s of data
  val context = ZMQ.context(1)

  //declare the PUBLISH socket
  val server = context.socket(ZMQ.PUB)
  //and bind it to an URL
  server.bind("tcp://*:5000")

  println("Starting to braodcast...")

  var i = 0
  while(true) {
    //on pub/sub message, the rooting is done
    //on the first word.
    //here, it's just "0, then 1, then 0, etc"
    val msg = s"${i%2} That's a message with ${i}\u0000".getBytes

    //only thing to do to broadcast the message to myriads of clients
    server.send(msg, 0)
    Thread.sleep(1000)
    i += 1
  }

}

/**
 * The SUBMIT client
 */
trait RawPubSubClient {
  def name : String
  def suscribeTo0 = false
  def suscribeTo1 = false

  val context = ZMQ.context(1)
  //that socket is a SUBSCRIBE one
  val client = context.socket(ZMQ.SUB)

  //connecting to our server (on localhost)
  client.connect("tcp://127.0.0.1:5000")
  if(suscribeTo0) client.subscribe("0".getBytes)
  if(suscribeTo1) client.subscribe("1".getBytes)
  //you can subscribe to all message with ""

  println(s"Client ${name} starts to read message from server")

  while(true) {
    //"receive" will wait until the next message comes
    val msg = client.recv(0)
    println(new String(msg, "UTF-8"))
  }
}

object PubSubClient1 extends App with RawPubSubClient {
  override def name = "client_1"
  override def suscribeTo0 = true
  override def suscribeTo1 = true
}
object PubSubClient2 extends App with RawPubSubClient {
  override def name = "client_2"
  override def suscribeTo1 = true
}
