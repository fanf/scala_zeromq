package psug.zeromq

import org.zeromq.ZMQ

object RawZmqServer extends App {

  val context = ZMQ.context(1)

  val server = context.socket(ZMQ.REP)
  server.bind("tcp://127.0.0.1:5000")

  println("Waiting for connections...")

  while(true) {
    val request = server.recv(0)
    val msg = new String(request, "UTF-8")
    println("Server received: " + msg)

    server.send("Again?".getBytes, 0)
  }

}

trait RawZmqClient {
  def name : String
  val context = ZMQ.context(1)
  val client = context.socket(ZMQ.REQ)
  client.connect("tcp://127.0.0.1:5000")

  println(s"Client ${name} starts to send message to server")

  while(true) {
    val request = client.send(("Hello from " + name).getBytes, 0)
    client.recv(0)
    Thread.sleep(1000)
  }
}

object client1 extends App with RawZmqClient { override def name = "client_1" }
object client2 extends App with RawZmqClient { override def name = "client_2" }
