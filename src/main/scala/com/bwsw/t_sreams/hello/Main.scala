package com.bwsw.t_sreams.hello


import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.env.{ConfigurationOptions, TStreamsFactory}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerBuilder
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{CommitLogOptions, StorageOptions}

import scala.util.Random

object Setup {
  val TOTAL_TRANSACTIONS = 100000
  val TOTAL_ITEMS = 1
  val TOTAL_PARTS = 10
  val STREAM_NAME = "hello"
  val PARTS = (0 until TOTAL_PARTS).toSet

  // create factory
  val factory = new TStreamsFactory()
  factory.setProperty(ConfigurationOptions.Stream.name, STREAM_NAME)

  def main(args: Array[String]): Unit = {
    val storageClient = factory.getStorageClient()
    val ttl = 24 * 3600
    if(storageClient.checkStreamExists(STREAM_NAME))
      storageClient.deleteStream(STREAM_NAME)
    storageClient.createStream(STREAM_NAME, TOTAL_PARTS, ttl, "")
    storageClient.shutdown()
    println(s"Setup is complete. The stream '$STREAM_NAME' with $TOTAL_PARTS partitions and TTL=$ttl created.")
    System.exit(0)
  }
}

/**
  * Created by Ivan Kudryavtsev on 05.08.16.
  */
object HelloProducer {
  def main(args: Array[String]): Unit = {

    // create producer
    val producer = Setup.factory.getProducer(
                name = "test_producer",
                partitions = Setup.PARTS)

    val startTime = System.currentTimeMillis()
    var sum = 0L

    (0 until Setup.TOTAL_TRANSACTIONS).foreach(
      i => {
        val t = producer.newTransaction()

        (0 until Setup.TOTAL_ITEMS).foreach(j => {
          val value = Random.nextInt()
          t.send(s"$value".getBytes())
          sum += value
        })
        if (i % 100 == 0)
          println(i)

        t.checkpoint()  // checkpoint the transaction
      })

    val stopTime = System.currentTimeMillis()
    println(s"Execution time is: ${stopTime - startTime}, sum: $sum")
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
    var transactionCounter = 0
    var sum = 0L

    val subscriber = Setup.factory.getSubscriber(
      name          = "test_subscriber",              // name of the subscribing consumer
      partitions    = Setup.PARTS,                    // active partitions
      offset        = Oldest,                         // it will start from newest available partitions
      useLastOffset = false,                          // will ignore history
      checkpointAtStart = true,
      callback = (op: TransactionOperator, txn: ConsumerTransaction) => this.synchronized {
        txn.getAll.foreach(i => sum += Integer.parseInt(new String(i))) // get all information from transaction

        transactionCounter += 1
        if (transactionCounter % 100 == 0) {
          println(transactionCounter)
          op.checkpoint()
        }
        if (transactionCounter == Setup.TOTAL_TRANSACTIONS) // if the producer sent all information, then end
          l.countDown()
      })

    subscriber.start() // start subscriber to operate
    val startTime = System.currentTimeMillis()
    l.await()
    val stopTime = System.currentTimeMillis()
    subscriber.stop() // stop operation
    println(s"Execution time is: ${stopTime - startTime}, sum: $sum")

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
    // never executed
    // transactionServer.shutdown()
    System.exit(0)
  }
}


