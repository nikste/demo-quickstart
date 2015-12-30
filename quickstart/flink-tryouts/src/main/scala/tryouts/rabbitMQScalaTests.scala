package tryouts

import akka.actor.{Props, Actor}
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{Amqp, Consumer, ConnectionOwner}
import com.rabbitmq.client.ConnectionFactory

/**
  * Created by nikste on 07.12.15.
  */
object rabbitMQScalaTests {
  def main(args:Array[String]): Unit ={
    val connFactory = new ConnectionFactory()
    connFactory.setUri("amqp://guest:guest@localhost/%2F")
    val conn = system.actorOf(ConnectionOwner.props(connFactory, 1 second))

    var i = 0
    // create an actor that will receive AMQP deliveries
    val listener = system.actorOf(Props(new Actor {
      def receive = {
        case Delivery(consumerTag, envelope, properties, body) => {
          println("hello" + i +  "\t" + new String(body))
          i += 1
          sender ! Ack(envelope.getDeliveryTag)
        }
      }
    }))

    // create a consumer that will route incoming AMQP messages to our listener
    // it starts with an empty list of queues to consume from
    val consumer = ConnectionOwner.createChildActor(conn, Consumer.props(listener, channelParams = None, autoack = false))

    // wait till everyone is actually connected to the broker
    Amqp.waitForConnection(system, consumer).await()

    // create a queue, bind it to a routing key and consume from it
    // here we don't wrap our requests inside a Record message, so they won't replayed when if the connection to
    // the broker is lost: queue and binding will be gone

    // create a queue
    val queueParams = QueueParameters("my_queue", passive = false, durable = false, exclusive = false, autodelete = true)
    consumer ! DeclareQueue(queueParams)
    // bind it
    consumer ! QueueBind(queue = "my_queue", exchange = "amq.direct", routing_key = "my_key")
    // tell our consumer to consume from it
    consumer ! AddQueue(QueueParameters(name = "my_queue", passive = false))
  }
}
