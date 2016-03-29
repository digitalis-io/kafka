/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.kafka

import org.apache.mesos.Protos.TaskState
import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.util
import scala.collection.JavaConversions._
import java.io.{ByteArrayOutputStream, PrintStream}
import Util.Period

class CliTest extends MesosTestCase {
  val out: ByteArrayOutputStream = new ByteArrayOutputStream()

  @Before
  override def before {
    super.before

    startHttpServer()
    Cli.api = Config.api
    Cli.out = new PrintStream(out, true)

    startZkServer()
  }

  @After
  override def after {
    Cli.out = System.out
    stopHttpServer()
    super.after
    stopZkServer()
  }

  @Test
  def help {
    exec("help")
    assertOutContains("Usage:")
    assertOutContains("scheduler")
    assertOutContains("broker")
    assertOutContains("topic")

    // command help
    for (command <- "help scheduler broker topic".split(" ")) {
      exec("help " + command)
      assertOutContains("Usage: " + command)
    }
  }

  @Test
  def broker_list{
    Nodes.addBroker(new Broker("0", testCluster))
    Nodes.addBroker(new Broker("1", testCluster))
    Nodes.addBroker(new Broker("2", testCluster))

    exec("broker list")
    assertOutContains("brokers:")
    assertOutContains("id: 0")
    assertOutContains("id: 1")
    assertOutContains("id: 2")

    // when broker needs restart
    val broker = Nodes.getBroker("0")
    broker.needsRestart = true
    exec("broker list")
    assertOutContains("(modified, needs restart)")
  }

  @Test
  def broker_add {
    exec("broker add 0 --cpus=0.1 --mem=128 --cluster default")
    assertOutContains("broker added:")
    assertOutContains("id: 0")
    assertOutContains("cpus:0.10, mem:128")

    assertEquals(1, Nodes.getBrokers.size)
    val broker = Nodes.getBroker("0")
    assertEquals(0.1, broker.cpus, 0.001)
    assertEquals(128, broker.mem)
  }

  @Test
  def broker_update {
    val broker = Nodes.addBroker(new Broker("0", testCluster))

    exec("broker update 0 --failover-delay=10s --failover-max-delay=20s --options=log.dirs=/tmp/kafka-logs")
    assertOutContains("broker updated:")
    assertOutContains("delay:10s, max-delay:20s")
    assertOutContains("options: log.dirs=/tmp/kafka-logs")

    assertEquals(new Period("10s"), broker.failover.delay)
    assertEquals(new Period("20s"), broker.failover.maxDelay)
    assertEquals(Util.parseMap("log.dirs=/tmp/kafka-logs"), broker.options)
  }

  @Test
  def broker_remove {
    Nodes.addBroker(new Broker("0", testCluster))
    exec("broker remove 0")

    assertOutContains("broker 0 removed")
    assertNull(Nodes.getBroker("0"))
  }

  @Test
  def broker_start_stop {
    val broker0 = Nodes.addBroker(new Broker("0", testCluster))
    val broker1 = Nodes.addBroker(new Broker("1", testCluster))

    exec("broker start * --timeout=0")
    assertOutContains("brokers scheduled to start:")
    assertOutContains("id: 0")
    assertOutContains("id: 1")
    assertTrue(broker0.active)
    assertTrue(broker1.active)

    exec("broker stop 0 --timeout=0")
    assertOutContains("broker scheduled to stop:")
    assertOutContains("id: 0")
    assertFalse(broker0.active)
    assertTrue(broker1.active)

    exec("broker stop 1 --timeout=0")
    assertOutContains("broker scheduled to stop:")
    assertOutContains("id: 1")
    assertFalse(broker0.active)
    assertFalse(broker1.active)
  }

  @Test
  def broker_start_stop_timeout {
    val broker = Nodes.addBroker(new Broker("0", testCluster))
    try { exec("broker start 0 --timeout=1ms"); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("broker start timeout")) }
    assertTrue(broker.active)

    broker.task = new Broker.Task("id", "slave", "executor", "host", _state = Broker.State.RUNNING)
    try { exec("broker stop 0 --timeout=1ms"); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("broker stop timeout")) }
    assertFalse(broker.active)
  }

  @Test(timeout = 60000)
  def broker_log: Unit = {
    // no broker
    assertCliErrorContains("broker log 0", "broker 0 not found")

    // broker isn't active or running
    val broker = Nodes.addBroker(new Broker("0", testCluster))
    assertCliErrorContains("broker log 0", "broker 0 is not active")

    broker.active = true
    assertCliErrorContains("broker log 0", "broker 0 is not running")

    // not running when task is null
    assertCliErrorContains("broker log 0", "broker 0 is not running")

    import Broker.State._

    broker.task = new Broker.Task("id", "slave", "executor", "host")
    for(state <- Seq(STOPPED, STARTING, RUNNING, RECONCILING, STOPPING) if state != RUNNING) {
      broker.task.state = state
      assertCliErrorContains("broker log 0", "broker 0 is not running")
    }

    def setLogContent(content: String, delay: Period = new Period("100ms")) =
      new Thread {
        override def run(): Unit = {
          Thread.sleep(delay.ms)
          Scheduler.logs.keys().take(1).foreach { rid => Scheduler.logs.put(rid, Some(content)) }
        }
      }.start()

    setLogContent("something")
    // retrieve log only for active and running broker
    broker.task.state = RUNNING
    try { exec("broker log 0 --timeout 1s") }
    catch { case e: Cli.Error => fail("") }

    assertOutContains("something")

    // with name
    setLogContent("something with name")
    exec("broker log 0 --name server.log --timeout 1s")
    assertOutContains("something with name")

    // with lines
    setLogContent("something with lines")
    exec("broker log 0 --lines 200 --timeout 1s")
    assertOutContains("something with lines")

    // with name, lines
    setLogContent("something with name with lines with timeout")
    exec("broker log 0 --name controller.log --lines 300 --timeout 1s")
    assertOutContains("something with name with lines with timeout")

    // timed out
    assertCliErrorContains("broker log 0 --timeout 1s", "broker 0 log retrieve timeout")

    // disconnected
    Scheduler.disconnected(schedulerDriver)
    assertCliErrorContains("broker log 0 --timeout 1s", "disconnected from the master")
  }

  @Test
  def broker_restart: Unit = {
    exec("help broker")
    assertOutContains("restart    - restart broker")

    exec("help broker restart")
    assertOutContains("Restart broker")
    assertOutContains("Usage: broker restart <broker-expr> [options]")
    assertOutContains("--timeout")

    val broker0 = Nodes.addBroker(new Broker("0", testCluster))
    val broker1 = Nodes.addBroker(new Broker("1", testCluster))

    def started(broker: Broker) {
      Scheduler.resourceOffers(schedulerDriver, Seq(offer(resources = "cpus:2.0;mem:2048;ports:9042..65000", hostname = "slave" + broker.id)))
      Scheduler.statusUpdate(schedulerDriver, taskStatus(id = broker.task.id, state = TaskState.TASK_RUNNING, data = "slave" + broker.id + ":9042"))
      assertEquals(Broker.State.RUNNING, broker.task.state)
    }

    def stopped(broker: Broker): Unit = {
      Scheduler.resourceOffers(schedulerDriver, Seq(offer(resources = "cpus:0.01;mem:128;ports:0..1")))
      Scheduler.statusUpdate(schedulerDriver, taskStatus(id = Broker.nextTaskId(broker), state = TaskState.TASK_FINISHED))
      assertFalse(broker.active)
      assertNull(broker.task)
    }

    for(broker <- Nodes.getBrokers) {
      exec("broker start " + broker.id + " --timeout 0s")
      started(broker)
    }

    // timeout
    assertCliErrorContains("broker restart * --timeout 200ms", "broker 0 timeout on stop")

    // restarted
    exec("broker start " + broker0.id + " --timeout 0s")
    started(broker0)

    delay("150ms") { stopped(broker0) }
    delay("250ms") { started(broker0) }
    delay("450ms") { stopped(broker1) }
    delay("650ms") { started(broker1) }
    exec("broker restart 0..1 --timeout 1s")
    assertOutContains("brokers restarted:")
    assertOutContains("id: 0")
    assertOutContains("id: 1")
  }

  @Test
  def topic_list {
    exec("topic list --cluster default")
    assertOutContains("no topics")

    testCluster.topics.addTopic("t0")
    testCluster.topics.addTopic("t1")
    testCluster.topics.addTopic("x")

    // list all
    exec("topic list --cluster default")
    assertOutContains("topics:")
    assertOutContains("t0")
    assertOutContains("t1")
    assertOutContains("x")

    // name filtering
    exec("topic list t* --cluster default")
    assertOutContains("t0")
    assertOutContains("t1")
    assertOutNotContains("x")
  }

  @Test
  def topic_add {
    exec("topic add t0 --cluster default")
    assertOutContains("topic added:")
    assertOutContains("name: t0")

    exec("topic list --cluster default")
    assertOutContains("topic:")
    assertOutContains("name: t0")
    assertOutContains("partitions: 0:[0]")

    exec("topic add t1 --partition 2 --cluster default")
    exec("topic list t1 --cluster default")
    assertOutContains("topic:")
    assertOutContains("name: t1")
    assertOutContains("partitions: 0:[0], 1:[0]")
  }

  @Test
  def topic_update {
    testCluster.topics.addTopic("t0")
    exec("topic update t0 --options=flush.ms=5000 --cluster default")
    assertOutContains("topic updated:")
    assertOutContains("name: t0")

    exec("topic list --cluster default")
    assertOutContains("topic:")
    assertOutContains("t0")
    assertOutContains("flush.ms=5000")
  }

  @Test
  def topic_rebalance {
    val rebalancer: Rebalancer = testCluster.rebalancer

    Nodes.addBroker(new Broker("0", testCluster))
    Nodes.addBroker(new Broker("1", testCluster))
    assertFalse(rebalancer.running)

    testCluster.topics.addTopic("t")
    exec("topic rebalance * --cluster default")
    assertTrue(rebalancer.running)
    assertOutContains("Rebalance started")
  }

  @Test
  def usage_errors {
    // no command
    try { exec(""); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("command required")) }

    // no id
    try { exec("broker add"); fail()  }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("argument required")) }

    // invalid command
    try { exec("unsupported 0"); fail()  }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("unsupported command")) }
  }

  @Test
  def connection_refused {
    HttpServer.stop()
    try {
      try { exec("broker add 0"); fail()  }
      catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("Connection refused")) }
    } finally {
      HttpServer.start()
    }
  }

  @Test
  def cluster_list{
    Nodes.addCluster(new Cluster("c1"))
    Nodes.addCluster(new Cluster("c2"))
    Nodes.addCluster(new Cluster("c3"))

    exec("cluster list")
    assertOutContains("clusters:")
    assertOutContains("id: c1")
    assertOutContains("id: c2")
    assertOutContains("id: c3")
  }

  @Test
  def cluster_add {
    exec("cluster add my_cluster --zk-connect zk://master:2181/kafka_1")
    assertOutContains("cluster added:")
    assertOutContains("id: my_cluster")
    assertOutContains("zk connection string: zk://master:2181/kafka_1")

    assertEquals(2, Nodes.getClusters.size)
    val cluster = Nodes.getCluster("my_cluster")
    assertEquals("zk://master:2181/kafka_1", cluster.zkConnect)
  }

  @Test
  def cluster_update {
    val cluster = new Cluster("my_cluster")
    cluster.zkConnect = "zk://master:2181/k1"
    Nodes.addCluster(cluster)

    exec(s"cluster update my_cluster --zk-connect zk://host:port/k1")
    assertOutContains("cluster updated:")
    assertOutContains("zk connection string: zk://host:port/k1")

    assertEquals("zk://host:port/k1", cluster.zkConnect)
  }

  @Test
  def cluster_remove {
    val cluster = new Cluster("my_cluster")
    cluster.zkConnect = "zk://master:2181/k1"
    Nodes.addCluster(cluster)
    exec("cluster remove my_cluster")

    assertOutContains("cluster my_cluster removed")
    assertNull(Nodes.getCluster("my_cluster"))
  }

  private def assertOutContains(s: String): Unit = assertTrue("" + out, out.toString.contains(s))
  private def assertOutNotContains(s: String): Unit = assertFalse("" + out, out.toString.contains(s))

  private def exec(cmd: String): Unit = {
    out.reset()

    val args = new util.ArrayList[String]()
    for (arg <- cmd.split(" "))
      if (!cmd.isEmpty) args.add(arg)
    Cli.exec(args.toArray(new Array[String](args.length)))
  }

  private def assertCliErrorContains(cmd: String, str: String) =
    try { exec(cmd); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains(str)) }
}
