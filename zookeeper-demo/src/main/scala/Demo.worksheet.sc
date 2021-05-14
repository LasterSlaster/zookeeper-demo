import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.slf4j.LoggerFactory
import org.apache.curator.framework.recipes.cache.NodeCacheListener
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.curator.retry.ExponentialBackoffRetry

val logger = LoggerFactory.getLogger(this.getClass.getName)

val retryPolicy = new ExponentialBackoffRetry(1000, 3)
val curatorZookeeperClient = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182,localhost:2183", retryPolicy)
curatorZookeeperClient.start
curatorZookeeperClient.getZookeeperClient.blockUntilConnectedOrTimedOut

val znodePath = "/test_node"
val originalData = new String(curatorZookeeperClient.getData.forPath(znodePath)) // This should be "Some data"

/* Zookeeper NodeCache service to get properties from ZNode */
val nodeCache = new NodeCache(curatorZookeeperClient, znodePath)

System.out.println()

nodeCache.getListenable.addListener(new NodeCacheListener {
  @Override
  def nodeChanged = {
    try {
      val dataFromZNode = nodeCache.getCurrentData()
      val newData = new String(dataFromZNode.getData) // This should be some new data after it is changed in the Zookeeper ensemble
    } catch {
      case ex: Exception => logger.error("Exception while fetching properties from zookeeper ZNode, reason " + ex.getCause)
    }
  }
  nodeCache.start
})

// Distributed Lock
// InterProcessMutex lock = new InterProcessMutex(client, lockPath);
// if ( lock.acquire(maxWait, waitUnit) ) 
// {
//     try 
//     {
//         // do some work inside of the critical section here
//     }
//     finally
//     {
//         lock.release();
//     }
// }

// Leader Election
// LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
// {
//     public void takeLeadership(CuratorFramework client) throws Exception
//     {
//         // this callback will get called when you are the leader
//         // do whatever leader work you need to and only exit
//         // this method when you want to relinquish leadership
//     }
// }

// LeaderSelector selector = new LeaderSelector(client, path, listener);
// selector.autoRequeue();  // not required, but this is behavior that you will probably expect
// selector.start();
// 
//ConnectionStateListener is called when there are connection disruptions. Clients can monitor these changes and take appropriate action. These are the possible state changes: 
//SUSPENDED 	There has been a loss of connection. Leaders, locks, etc. should suspend until the connection is re-established.
//RECONNECTED 	A suspended or lost connection has been re-established.
