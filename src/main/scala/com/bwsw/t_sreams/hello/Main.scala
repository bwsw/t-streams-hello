package com.bwsw.t_sreams.hello


import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.{Newest}
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.{ConfigurationOptions, TStreamsFactory}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerBuilder
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{CommitLogOptions, StorageOptions}

import scala.util.Random

object Setup {
  val TOTAL_TXNS = 100000
  val TOTAL_ITMS = 100
  val KS = "tk_hello"
  val TOTAL_PARTS = 10
  val PARTS = (0 until TOTAL_PARTS).toSet

  // create factory
  val factory = new TStreamsFactory()

  factory.setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
    setProperty(ConfigurationOptions.Producer.Transaction.batchSize, 1000).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10).
    setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Consumer.Subscriber.pollingFrequencyDelayMs, 1000).
    setProperty(ConfigurationOptions.Consumer.Subscriber.processingEnginesThreadPoolSize, 5).
    setProperty(ConfigurationOptions.Consumer.Subscriber.transactionBufferThreadPoolSize, 5).
    setProperty(ConfigurationOptions.Stream.partitionsCount, TOTAL_PARTS)

  def main(args: Array[String]): Unit = {
    val storageClient = factory.getStorageClient()
    storageClient.createStream("test_stream", TOTAL_PARTS, 24 * 3600, "")
    storageClient.shutdown()
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
    val producer = Setup.factory.getProducer(
                name = "test_producer",                     // name of the producer
                partitions = Setup.PARTS)                      // agent can be a master

    val startTime = System.currentTimeMillis()
    var sum = 0L

    (0 until Setup.TOTAL_TXNS).foreach(
      i => {
        val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened) // create new transaction
        (0 until Setup.TOTAL_ITMS).foreach(j => {
          val v = Random.nextInt()
          t.send(s"${v}".getBytes())
          sum += v
        })
        if (i % 100 == 0)
          println(i)

        t.checkpoint(true)  // checkpoint the transaction
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

    val subscriber = Setup.factory.getSubscriber(
      name          = "test_subscriber",              // name of the subscribing consumer
      partitions    = Setup.PARTS,                        // active partitions
      offset        = Newest,                         // it will start from newest available partitions
      useLastOffset = true,                        // will ignore history
      checkpointAtStart = true,
      callback = (op: TransactionOperator, txn: ConsumerTransaction) => this.synchronized {
        txn.getAll().foreach(i => sum += Integer.parseInt(new String(i))) // get all information from transaction
        cntr += 1
        if (cntr % 100 == 0) {
          println(cntr)
          op.checkpoint()
        }
        if (cntr == Setup.TOTAL_TXNS) // if the producer sent all information, then end
          l.countDown()
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

object Server {
  def main(args: Array[String]): Unit = {
    val serverBuilder = new ServerBuilder()
      .withZookeeperOptions(new ZookeeperOptions(endpoints = s"127.0.0.1:2181"))

    val transactionServer = serverBuilder
      .withServerStorageOptions(new StorageOptions(path = "/tmp"))
      .withCommitLogOptions(new CommitLogOptions(commitLogCloseDelayMs = 1000))
      .build()

    transactionServer.start()
    scala.io.StdIn.readLine()
    transactionServer.shutdown()
  }
}
