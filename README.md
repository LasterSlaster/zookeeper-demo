# ZooKeeper Demo

A simple demonstration of ZooKeeper in Scala with Apache Curator as a ZooKeeper
client abstraction.

Contains three ZooKeeper direcotrys with pre-configured ZooKeeper servers to setup a simple ensemble. Start each server with by navigatin into it's /bin directory and run `./zkServer.sh start`. This will start the server the with it's own configuration.
This step has to be executed befor running the scala demo application!

The Scala demo contains seperate Scala worksheets to tests different kind of ZooKeeper applications.

Author: Marius Degen
