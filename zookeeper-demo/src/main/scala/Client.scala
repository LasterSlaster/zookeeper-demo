import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.EventType

import java.io.DataInputStream
import java.net.Socket
import java.util.concurrent.CountDownLatch

object Client {
  def main(args: Array[String]): Unit = {

    val serverPath = "/server"
    val host = "localhost"
    val zkHosts = "localhost:2181,localhost:2182,localhost:2183"
    var serverPort = 1234

    val latch = new CountDownLatch(1)
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val zkClient = CuratorFrameworkFactory.newClient(zkHosts, retryPolicy)
    zkClient.start()

    val exists = zkClient.checkExists().usingWatcher(new CuratorWatcher {
      override def process(event: WatchedEvent): Unit = {
        if (event.getType eq EventType.NodeCreated) {
          println("--------------------------------First PrimeServer has started")
          latch.countDown()
        }
      }
    }).forPath(serverPath)

    if (exists == null) {
      println("--------------------------------Waiting for PrimeServer to start")
      latch.await()
    }

    // read masterAddress and set watch to get notified about when master changes
    val nodeCache = new NodeCache(zkClient, serverPath)
    nodeCache.getListenable.addListener(new NodeCacheListener {
      @Override
      def nodeChanged = {
        // master has changed - update port
        try {
          val dataFromZNode = nodeCache.getCurrentData
          serverPort = BigInt.apply(dataFromZNode.getData).intValue
          System.out.println("--------------------------------New master port:" + serverPort)
        } catch {
          case ex: Exception => System.out.println(s"--------------------------------Exception while fetching properties from zookeeper ZNode, reason ${ex.getCause}")
        }
      }
    })
    nodeCache.start()

    // Initially read port from ZK
    serverPort = BigInt.apply(zkClient.getData.forPath(serverPath)).intValue
    println("--------------------------------Server Port is:" + serverPort)

    while (!Thread.currentThread().isInterrupted) {
      println("--------------------------------Request next prime?")
      System.in.read()
      try {
        val socket = new Socket(host, serverPort)
        val input = new DataInputStream(socket.getInputStream)
        val primeNumber = BigInt.apply(input.read()).intValue
        println(primeNumber)
      } catch {
        case e: Exception => println("--------------------------------Server was disconnected")
      }
    }
  }
}
