import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListener, LeaderSelectorListenerAdapter}
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory

import java.util.concurrent.{CountDownLatch, TimeUnit}

val logger = LoggerFactory.getLogger("Demo.worksheet.sc")

val retryPolicy = new ExponentialBackoffRetry(1000, 3)
val curatorZookeeperClient = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182,localhost:2183", retryPolicy)
val latch = new CountDownLatch(1)
curatorZookeeperClient.start

if (curatorZookeeperClient.getZookeeperClient.blockUntilConnectedOrTimedOut) {

  // Read and write data to a ZNode
  val znodePath = "/test_node"
  val originalData = new String(curatorZookeeperClient.getData.forPath(znodePath)) // This should be "Some data"
  //logger.info(s"This should be ${"Some data"}$originalData")
  System.out.println(s"This is the originl data stored under ZNode $znodePath: $originalData")
  /* Zookeeper NodeCache service to get properties from ZNode */
  val nodeCache = new NodeCache(curatorZookeeperClient, znodePath)

  // Register a Watch
  nodeCache.getListenable.addListener(new NodeCacheListener {
    @Override
    def nodeChanged = {
      // trigger callback when znode data has been changed
      try {
        val dataFromZNode = nodeCache.getCurrentData()
        val newData = new String(dataFromZNode.getData) // This should be some new data after it is changed in the Zookeeper ensemble
        //logger.info(s"This should be some new data after it is changed in the Zookeeper ensemble: $newData")
        System.out.println(s"This should be some new data after it is changed in the Zookeeper ensemble: $newData")
      } catch {
        case ex: Exception => System.out.println(s"Exception while fetching properties from zookeeper ZNode, reason ${ex.getCause}")
      } finally {
        latch.countDown()
      }
    }
    nodeCache.start
  })
  // Write new data to ZNode
  curatorZookeeperClient.setData().forPath(znodePath, "new data".getBytes())
  // Block until data in ZNode has been changed / ZooKeeper Watch has been triggered and latch is count down)
  latch.await()

  // Distributed Lock
  val lockPath = "/lock"
  val maxWait = 1
  val waitUnit = TimeUnit.MILLISECONDS
  val lock = new InterProcessMutex(curatorZookeeperClient, lockPath)
  // Blocking call to try to access a restricted resource by acquiring the necessary lock guarding access within the distributed application
  if (lock.acquire(maxWait, waitUnit)) {
    // This client has acquired the lock and is now allowed to access the resource
    try {
      // Accessing the restricted resource
      System.out.println(s"Client(Thread) with ID ${Thread.currentThread().getId} has the lock acquired")
    }
    finally {
      // Release the lock to signal other clients that you're finished accessing the resource and that they can acquire the lock
      lock.release()
    }
  }

  // Leader Election
  val listener: LeaderSelectorListener = new LeaderSelectorListenerAdapter() {
    def takeLeadership(client: CuratorFramework): Unit = {
      // this callback will get called when you are the leader
      // do whatever leader work you need to and only exit
      // this method when you want to relinquish leadership
      System.out.println("Look at me! I'm the captain, now.")
      System.out.println(Thread.currentThread().getId)
    }
  }
  val leaderPath = "/leader"
  val selector: LeaderSelector = new LeaderSelector(curatorZookeeperClient, leaderPath, listener)
  // Automatically requeue for leader election after leadership has been relinquished
  selector.autoRequeue() // not required, but this is behavior that you will probably expect
  // Start to participate as a potential leader in leader group "leaderPath"(ZNode)
  selector.start()

} else {
  //logger.warn("Unable to connect to a zookeeper cluster. Make sure that you have at least one server instance runnding under one of: localhost:2181,localhost:2182,localhost:2183")
  System.out.println("Unable to connect to a zookeeper cluster. Make sure that you have at least one server instance runnding under one of: localhost:2181,localhost:2182,localhost:2183")
}
//ConnectionStateListener is called when there are connection disruptions. Clients can monitor these changes and take appropriate action. These are the possible state changes:
//SUSPENDED 	There has been a loss of connection. Leaders, locks, etc. should suspend until the connection is re-established.
//RECONNECTED 	A suspended or lost connection has been re-established.

// Potential use cases
// - Lock: Manage access to a database
// - Messaging System?

// Use cases: Barrier to form a group for an operation and than coordinate access to a resource with a lock..?!
// Split a heavy computation between distributed servers: Begin with a barrier, use a lock to wait for results, maybe integrate a cache to share results
//