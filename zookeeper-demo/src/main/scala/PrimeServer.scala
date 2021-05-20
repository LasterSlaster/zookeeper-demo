import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

import java.net.ServerSocket
import java.util.concurrent.CountDownLatch

object PrimeServer {
  def main(args: Array[String]): Unit = {

    val masterPath = "/master"
    val barrierPath = "/barrier"
    val lastPrimePath = "/lastprime"
    val serverPath = "/server"
    val serverPort = args(0).toInt
    val zkHosts = "localhost:2181,localhost:2182,localhost:2183"

    val serverSocket = new ServerSocket(serverPort)

    val latch = new CountDownLatch(1)
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val zkClient = CuratorFrameworkFactory.newClient(zkHosts, retryPolicy)
    zkClient.start()

    val barrier = new DistributedDoubleBarrier(zkClient, barrierPath, 3)
    // wait on enough PrimeServers to join
    val barrierCount = (zkClient.getChildren.forPath(barrierPath).size() - 2).abs
    println(s"--------------------------------Waiting on $barrierCount more servers to join the barrier")
    barrier.enter() // blocking!
    println("--------------------------------3 PrimeServers are now available")

    // Define what to do when leader
    val listener = new LeaderSelectorListenerAdapter {
      override def takeLeadership(client: CuratorFramework): Unit = {
        // Now, I'm the master
        println("--------------------------------I'm the captain now!")

        // Publish new master address
        val exists = zkClient.checkExists.forPath(serverPath)
        if (exists == null) {
          println("--------------------------------Creating new master zNode")
          zkClient.create.withMode(CreateMode.EPHEMERAL).forPath(serverPath, BigInt(serverPort).toByteArray)
        } else {
          println("--------------------------------Updating master zNode data with new server address")
          zkClient.setData().forPath(serverPath, BigInt(serverPort).toByteArray)
        }

        // Start to serve prime numbers
        var nextPrime = 1
        val fistPrime = zkClient.checkExists.forPath(lastPrimePath)
        if (fistPrime == null) {
          nextPrime = 1
          zkClient.create().forPath(lastPrimePath, BigInt(1).toByteArray)
          println(s"--------------------------------Starting prime calculation with 1")
        } else {
          nextPrime = BigInt.apply(zkClient.getData.forPath(lastPrimePath)).intValue
          println(s"--------------------------------Continuing to calculate primes beginning with $nextPrime")
        }

        val limit = Integer.MAX_VALUE
        for (i <- nextPrime to limit if isPrime(i)) {
          println(s"--------------------------------Waiting on client to request a prime")
          val socket = serverSocket.accept
          socket.getOutputStream.write(BigInt(i).toByteArray)
          socket.getOutputStream.flush()
          socket.close()
          println(s"--------------------------------Send prime $i to client")

          zkClient.setData().forPath(lastPrimePath, BigInt(i).toByteArray)
          println(s"--------------------------------Updated Zookeeper with last calculated prime")
        }
      }
    }


    println(s"--------------------------------Enqueue to leader election")
    val selector = new LeaderSelector(zkClient, masterPath, listener)
    // Enqueue to leader selection
    selector.start()

    latch.await()
  }

  def isPrime(n:Integer): Boolean = ! ((2 until n-1) exists (n % _ == 0))
}
