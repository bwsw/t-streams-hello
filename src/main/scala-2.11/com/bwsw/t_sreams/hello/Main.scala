package com.bwsw.t_sreams.hello

import java.util.UUID
import java.util.concurrent.CountDownLatch
import com.bwsw.tstreams.agents.consumer.Offsets.{Newest, Oldest}
import com.bwsw.tstreams.agents.consumer.subscriber.{SubscribingConsumer, Callback}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.{TStreamsFactory, TSF_Dictionary}
import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator
import com.bwsw.tstreams.converter.{StringToArrayByteConverter, ArrayByteToStringConverter}

/**
  * Created by ivan on 05.08.16.
  */
object HelloProducer {
  def main(args: Array[String]): Unit = {

    // create factory
    val f = new TStreamsFactory()
    f.setProperty(TSF_Dictionary.Metadata.Cluster.NAMESPACE, "tk_1").                 // keyspace must exist in C*
      setProperty(TSF_Dictionary.Data.Cluster.NAMESPACE, "test").                     // exists by default in Aerospike
      setProperty(TSF_Dictionary.Producer.BIND_PORT, 18001).                          // producer will listen localhost:18001
      setProperty(TSF_Dictionary.Consumer.Subscriber.BIND_PORT, 18002).               // subscriber will listen localhost:18002
      setProperty(TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH, "/tmp").  // subscriber will store data bursts in /tmp
      setProperty(TSF_Dictionary.Stream.NAME, "test-stream")                          // producer and consumer will operate on "test-stream" t-stream

    val l = new CountDownLatch(1)
    var cntr = 0
    val TOTAL_TXNS = 10000
    val TOTAL_ITMS = 1

    // create producer
    val producer = f.getProducer[String](
                name = "test_producer",                     // name of the producer
                txnGenerator = new LocalTimeUUIDGenerator,  // where it will get new transactions
                converter = new StringToArrayByteConverter, // converter from String to internal data presentation
                partitions = List(0),                       // active partitions
                isLowPriority = false)                      // agent can be a master

    val startTime = System.currentTimeMillis()

    (0 until TOTAL_TXNS).foreach(
      i => {
        val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened) // create new transaction
        (0 until TOTAL_ITMS).foreach(j => t.send(s"I: ${i}, J: ${j}"))
        if (i % 100 == 0)
          println(i)
        t.checkpoint(false)  // checkpoint the transaction
      })

    val stopTime = System.currentTimeMillis()
    println(s"Execution time is: ${stopTime - startTime}")
    producer.stop()   // stop operation
    f.close()         // end operation
    System.exit(0)
  }
}

/**
  * Created by ivan on 05.08.16.
  */
object HelloSubscriber {
  def main(args: Array[String]): Unit = {

    // create factory
    val f = new TStreamsFactory()
    f.setProperty(TSF_Dictionary.Metadata.Cluster.NAMESPACE, "tk_1").                 // keyspace must exist in C*
      setProperty(TSF_Dictionary.Data.Cluster.NAMESPACE, "test").                     // exists by default in Aerospike
      setProperty(TSF_Dictionary.Producer.BIND_PORT, 18001).                          // producer will listen localhost:18001
      setProperty(TSF_Dictionary.Consumer.Subscriber.BIND_PORT, 18002).               // subscriber will listen localhost:18002
      setProperty(TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH, "/tmp").  // subscriber will store data bursts in /tmp
      setProperty(TSF_Dictionary.Stream.NAME, "test-stream")                          // producer and consumer will operate on "test-stream" t-stream

    val l = new CountDownLatch(1)
    var cntr = 0
    val TOTAL_TXNS = 10000

    val subscriber = f.getSubscriber[String](
      name          = "test_subscriber",              // name of the subscribing consumer
      txnGenerator  = new LocalTimeUUIDGenerator,     // where it can get transaction uuids
      converter     = new ArrayByteToStringConverter, // vice versa converter to string
      partitions    = List(0),                        // active partitions
      offset        = Newest,                         // it will start from newest available partitions
      isUseLastOffset = false,                        // will ignore history
      callback = new Callback[String] {
        override def onEvent(subscriber: SubscribingConsumer[String], partition: Int, transactionUuid: UUID): Unit = {
          val txn = subscriber.getTransactionById(partition, transactionUuid) // get transaction
          txn.get.getAll().foreach(i => i)                           // get all information from transaction
          cntr += 1
          if (cntr % 100 == 0)
            println(cntr)
          if(cntr == TOTAL_TXNS)                                              // if the producer sent all information, then end
            l.countDown()
        }
      })

    subscriber.start() // start subscriber to operate

    val startTime = System.currentTimeMillis()

    l.await()
    subscriber.stop() // stop operation
    f.close()         // end operation
    System.exit(0)
  }
}