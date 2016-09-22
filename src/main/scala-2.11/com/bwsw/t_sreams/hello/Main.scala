package com.bwsw.t_sreams.hello

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.{CassandraConnectorConf, CassandraHelper, MetadataConnectionPool}
import com.bwsw.tstreams.env.{TSF_Dictionary, TStreamsFactory}
import com.bwsw.tstreams.generator.LocalTransactionGenerator
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}

import scala.util.Random

object Setup {
  val TOTAL_TXNS = 100000
  val TOTAL_ITMS = 1
  val KS = "tk_hello"
  val TOTAL_PARTS = 10
  val PARTS = (0 until TOTAL_PARTS).toSet

  // create factory
  val factory = new TStreamsFactory()

  factory
    .setProperty(TSF_Dictionary.Metadata.Cluster.NAMESPACE, Setup.KS)
    .setProperty(TSF_Dictionary.Data.Cluster.NAMESPACE, "test")
    .setProperty(TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH, null)
    .setProperty(TSF_Dictionary.Stream.NAME, "test-stream")
    .setProperty(TSF_Dictionary.Consumer.Subscriber.POLLING_FREQUENCY_DELAY, 1000)
    .setProperty(TSF_Dictionary.Stream.PARTITIONS, TOTAL_PARTS)
    .setProperty(TSF_Dictionary.Consumer.Subscriber.TRANSACTION_BUFFER_THREAD_POOL, 5)
    .setProperty(TSF_Dictionary.Consumer.Subscriber.PROCESSING_ENGINES_THREAD_POOL, 5)
    .setProperty(TSF_Dictionary.Coordination.ENDPOINTS, "localhost:2181")
    .setProperty(TSF_Dictionary.Metadata.Cluster.ENDPOINTS, "localhost:9042")
    .setProperty(TSF_Dictionary.Data.Cluster.ENDPOINTS, "localhost:3000")

  def main(args: Array[String]): Unit = {
    val cluster = MetadataConnectionPool.getCluster(CassandraConnectorConf(Set(new InetSocketAddress("localhost", 9042))))
    val session = MetadataConnectionPool.getSession(CassandraConnectorConf(Set(new InetSocketAddress("localhost", 9042))), null)

    CassandraHelper.createKeyspace(session, Setup.KS)
    CassandraHelper.createMetadataTables(session, Setup.KS)
    CassandraHelper.createDataTable(session, Setup.KS)
    CassandraHelper.clearMetadataTables(session, Setup.KS)
    CassandraHelper.clearDataTable(session, Setup.KS)

    session.close()
    cluster.close()
  }
}
/**
  * Created by Ivan Kudryavtsev on 05.08.16.
  */
object HelloProducer {
  def main(args: Array[String]): Unit = {
    val l = new CountDownLatch(1)
    var cntr = 0

    // create producer
    val producer = Setup.factory.getProducer[String](
                name = "test_producer",                     // name of the producer
                transactionGenerator = new LocalTransactionGenerator(),  // where it will get new transactions
                converter = new StringToArrayByteConverter, // converter from String to internal data presentation
                partitions = Setup.PARTS,                       // active partitions
                isLowPriority = false)                      // agent can be a master

    val startTime = System.currentTimeMillis()
    var sum = 0L

    (0 until Setup.TOTAL_TXNS).foreach(
      i => {
        val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened) // create new transaction
        (0 until Setup.TOTAL_ITMS).foreach(j => {
          val v = Random.nextInt()
          t.send(s"${v}")
          sum += v
        })
        if (i % 100 == 0)
          println(i)
        t.checkpoint(false)  // checkpoint the transaction
      })

    val stopTime = System.currentTimeMillis()
    println(s"Execution time is: ${stopTime - startTime}, sum: ${sum}")
    producer.stop()   // stop operation
    System.exit(0)
  }
}

/**
  * Created by Ivan Kudryavtsev on 05.08.16.
  */
object HelloSubscriber {
  def main(args: Array[String]): Unit = {

    val l = new CountDownLatch(1)
    var cntr = 0
    var sum = 0L

    val subscriber = Setup.factory.getSubscriber[String](
      name          = "test_subscriber",              // name of the subscribing consumer
      transactionGenerator  = new LocalTransactionGenerator(),     // where it can get transaction uuids
      converter     = new ArrayByteToStringConverter, // vice versa converter to string
      partitions    = Setup.PARTS,                        // active partitions
      offset        = Newest,                         // it will start from newest available partitions
      isUseLastOffset = false,                        // will ignore history
      callback = new Callback[String] {
        override def onTransaction(op: TransactionOperator[String], txn: ConsumerTransaction[String]): Unit = this.synchronized {
          txn.getAll().foreach(i => sum += Integer.parseInt(i))                           // get all information from transaction
          cntr += 1
          if (cntr % 100 == 0) {
            println(cntr)
            op.checkpoint()
          }
          if(cntr == Setup.TOTAL_TXNS)                                              // if the producer sent all information, then end
            l.countDown()
        }
      })

    subscriber.start() // start subscriber to operate
    val startTime = System.currentTimeMillis()
    l.await()
    val stopTime = System.currentTimeMillis()
    subscriber.stop() // stop operation
    println(s"Execution time is: ${stopTime - startTime}, sum: ${sum}")

    System.exit(0)
  }
}
