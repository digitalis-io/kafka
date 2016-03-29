package ly.stealth.mesos.kafka

import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.util

class ExprTest extends MesosTestCase {
  @Before
  override def before {
    super.before
    startZkServer()
  }

  @After
  override def after {
    super.after
    stopZkServer()
  }

  @Test
  def expandBrokers {
    for (i <- 0 until 5)
      Nodes.addBroker(new Broker("" + i, testCluster))

    try {
      assertEquals(util.Arrays.asList(), Expr.expandBrokers(""))
      fail()
    } catch { case e: IllegalArgumentException => }

    assertEquals(util.Arrays.asList("0"), Expr.expandBrokers("0"))
    assertEquals(util.Arrays.asList("0", "2", "4"), Expr.expandBrokers("0,2,4"))

    assertEquals(util.Arrays.asList("1", "2", "3"), Expr.expandBrokers("1..3"))
    assertEquals(util.Arrays.asList("0", "1", "3", "4"), Expr.expandBrokers("0..1,3..4"))

    assertEquals(util.Arrays.asList("0", "1", "2", "3", "4"), Expr.expandBrokers("*"))

    // duplicates
    assertEquals(util.Arrays.asList("0", "1", "2", "3", "4"), Expr.expandBrokers("0..3,2..4"))

    // sorting
    assertEquals(util.Arrays.asList("2", "3", "4"), Expr.expandBrokers( "4,3,2"))

    // not-existent brokers
    assertEquals(util.Arrays.asList("5", "6", "7"), Expr.expandBrokers("5,6,7"))
  }

  @Test
  def expandBrokers_attributes {
    val b0 = Nodes.addBroker(new Broker("0", testCluster))
    val b1 = Nodes.addBroker(new Broker("1", testCluster))
    val b2 = Nodes.addBroker(new Broker("2", testCluster))
    Nodes.addBroker(new Broker("3", testCluster))

    b0.task = new Broker.Task(_hostname = "master", _attributes = Util.parseMap("a=1"))
    b1.task = new Broker.Task(_hostname = "slave0", _attributes = Util.parseMap("a=2,b=2"))
    b2.task = new Broker.Task(_hostname = "slave1", _attributes = Util.parseMap("b=2"))

    // exact match
    assertEquals(util.Arrays.asList("0", "1", "2", "3"), Expr.expandBrokers("*"))
    assertEquals(util.Arrays.asList("0"), Expr.expandBrokers("*[a=1]"))
    assertEquals(util.Arrays.asList("1", "2"), Expr.expandBrokers("*[b=2]"))

    // attribute present
    assertEquals(util.Arrays.asList("0", "1"), Expr.expandBrokers("*[a]"))
    assertEquals(util.Arrays.asList("1", "2"), Expr.expandBrokers("*[b]"))

    // hostname
    assertEquals(util.Arrays.asList("0"), Expr.expandBrokers("*[hostname=master]"))
    assertEquals(util.Arrays.asList("1", "2"), Expr.expandBrokers("*[hostname=slave*]"))

    // not existent broker
    assertEquals(util.Arrays.asList(), Expr.expandBrokers("5[a]"))
    assertEquals(util.Arrays.asList(), Expr.expandBrokers("5[]"))
  }

  @Test
  def expandBrokers_sortByAttrs {
    val b0 = Nodes.addBroker(new Broker("0", testCluster))
    val b1 = Nodes.addBroker(new Broker("1", testCluster))
    val b2 = Nodes.addBroker(new Broker("2", testCluster))
    val b3 = Nodes.addBroker(new Broker("3", testCluster))
    val b4 = Nodes.addBroker(new Broker("4", testCluster))
    val b5 = Nodes.addBroker(new Broker("5", testCluster))

    b0.task = new Broker.Task(_attributes = Util.parseMap("r=2,a=1"))
    b1.task = new Broker.Task(_attributes = Util.parseMap("r=0,a=1"))
    b2.task = new Broker.Task(_attributes = Util.parseMap("r=1,a=1"))
    b3.task = new Broker.Task(_attributes = Util.parseMap("r=1,a=2"))
    b4.task = new Broker.Task(_attributes = Util.parseMap("r=0,a=2"))
    b5.task = new Broker.Task(_attributes = Util.parseMap("r=0,a=2"))

    assertEquals(util.Arrays.asList("0", "1", "2", "3", "4", "5"), Expr.expandBrokers("*", sortByAttrs = true))
    assertEquals(util.Arrays.asList("1", "2", "0", "4", "3", "5"), Expr.expandBrokers("*[r]", sortByAttrs = true))
    assertEquals(util.Arrays.asList("1", "4", "2", "3", "0", "5"), Expr.expandBrokers("*[r,a]", sortByAttrs = true))

    assertEquals(util.Arrays.asList("1", "2", "0"), Expr.expandBrokers("*[r=*,a=1]", sortByAttrs = true))
    assertEquals(util.Arrays.asList("4", "3", "5"), Expr.expandBrokers("*[r,a=2]", sortByAttrs = true))
  }

  @Test
  def expandTopics {
    val topics: Topics = testCluster.topics

    topics.addTopic("t0")
    topics.addTopic("t1")
    topics.addTopic("x")

    assertEquals(util.Arrays.asList(), Expr.expandTopics("", testCluster))
    assertEquals(util.Arrays.asList("t5", "t6"), Expr.expandTopics("t5,t6", testCluster))
    assertEquals(util.Arrays.asList("t0"), Expr.expandTopics("t0", testCluster))
    assertEquals(util.Arrays.asList("t0", "t1"), Expr.expandTopics("t0, t1", testCluster))
    assertEquals(util.Arrays.asList("t0", "t1", "x"), Expr.expandTopics("*", testCluster))
    assertEquals(util.Arrays.asList("t0", "t1"), Expr.expandTopics("t*", testCluster))
  }
}
