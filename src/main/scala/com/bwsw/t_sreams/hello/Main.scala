package com.bwsw.t_sreams.hello


import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.{Oldest, Newest}
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.{ConfigurationOptions, TStreamsFactory}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerBuilder
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{CommitLogOptions, StorageOptions}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Setup {
  val TOTAL_TXNS = 100000
  val TOTAL_ITMS = 1
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
    setProperty(ConfigurationOptions.Producer.Transaction.batchSize, 100).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10).
    setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Consumer.Subscriber.pollingFrequencyDelayMs, 1000).
    setProperty(ConfigurationOptions.Consumer.Subscriber.processingEnginesThreadPoolSize, 5).
    setProperty(ConfigurationOptions.Consumer.Subscriber.transactionBufferThreadPoolSize, 5).
    setProperty(ConfigurationOptions.Stream.partitionsCount, TOTAL_PARTS)

  def main(args: Array[String]): Unit = {
    val storageClient = factory.getStorageClient()
    //storageClient.deleteStream("test_stream")
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
      useLastOffset = false,                        // will ignore history
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


/**
  * Created by Ivan Kudryavtsev on 05.08.16.
  */
object ProducerTimes {
  def main(args: Array[String]): Unit = {

    Setup.factory.setProperty(ConfigurationOptions.Producer.Transaction.batchSize, 1)

    // create producer
    val producer = Setup.factory.getProducer(
      name = "test_producer",                     // name of the producer
      partitions = Setup.PARTS)                      // agent can be a master

    val N = 10000

    val avgNewTransaction = new Counters("New Transaction", N)
    var avgSendData = new Counters("Send Data", N)
    var avgCheckpoint = new Counters("Checkpoint", N)

    (0 until N).foreach(counter => {

      val t1 = System.currentTimeMillis()

      val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened) // create new transaction
      val t2 = System.currentTimeMillis()

      t.send(new Array[Byte](100))
      val t3 = System.currentTimeMillis()

      t.checkpoint(true)
      val t4 = System.currentTimeMillis()

      avgNewTransaction += t2 - t1
      avgSendData += t3-t2
      avgCheckpoint += t4 - t3

      if(counter % 1000 == 0)
        println(s"Produced $counter items")

    })

    println(avgNewTransaction)
    println(avgSendData)
    println(avgCheckpoint)

    producer.stop()   // stop operation
    System.exit(0)
  }
}


class Counters(val label: String, val size: Int) {
  var min = Long.MaxValue
  var max = Long.MinValue
  var avgSum = 0L
  var buf = ListBuffer[Long]()

  def += (itm: Long) = {
    if(min > itm) min = itm
    if(max < itm) max = itm
    avgSum += itm
    buf.append(itm)
  }

  override def toString() = {
    val sorted = buf.toArray.sorted
    val percentileIt90 = sorted((size * .90f).toInt - 1)
    val percentileIt95 = sorted((size * .95f).toInt - 1)
    val percentileIt97 = sorted((size * .97f).toInt - 1)
    val percentileIt98 = sorted((size * .98f).toInt - 1)
    val percentileIt99 = sorted((size * .99f).toInt - 1)
    s"$label (min=$min, max=$max, avg=${avgSum / size}, 90%=$percentileIt90, 95%=$percentileIt95, 97%=$percentileIt97, 98%=$percentileIt98, 99%=$percentileIt99)\n"
  }

}