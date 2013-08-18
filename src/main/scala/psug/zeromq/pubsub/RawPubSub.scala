package psug.zeromq
package pubsub

import org.zeromq.ZMQ

object RawPubSubServer extends App {

  val context = ZMQ.context(1)

  val server = context.socket(ZMQ.PUB)
  server.bind("tcp://127.0.0.1:5000")

  println("Starting to braodcast...")

  var i = 0
  while(true) {
    //the first
    val msg = s"${i%2} That's a message with ${i}\u0000".getBytes

    server.send(msg, 0)
    Thread.sleep(1000)
    i += 1
  }

}

trait RawPubSubClient {
  def name : String
  def suscribeTo0 = false
  def suscribeTo1 = false

  val context = ZMQ.context(1)
  val client = context.socket(ZMQ.SUB)
  client.connect("tcp://127.0.0.1:5000")
  if(suscribeTo0) client.subscribe("0".getBytes)
  if(suscribeTo1) client.subscribe("1".getBytes)

  println(s"Client ${name} starts to read message from server")

  while(true) {
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
