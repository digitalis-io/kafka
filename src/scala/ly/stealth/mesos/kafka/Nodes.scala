package ly.stealth.mesos.kafka

import java.io.File

import org.apache.log4j.Logger

import scala.collection.mutable
import scala.util.parsing.json.{JSONArray, JSONObject}

object Nodes {
  private[kafka] val logger = Logger.getLogger(this.getClass)
  private[kafka] var storage = newStorage(Config.storage)

  var frameworkId: String = null
  val clusters = new mutable.ListBuffer[Cluster]
  val brokers = new mutable.ListBuffer[Broker]

  reset()

  def getClusters: List[Cluster] = clusters.toList

  def getCluster(id: String): Cluster = clusters.find(id == _.id).orNull

  def addCluster(cluster: Cluster): Cluster = {
    if (getCluster(cluster.id) != null)
      throw new IllegalArgumentException(s"duplicate cluster ${cluster.id}")

    clusters += cluster
    cluster
  }

  def removeCluster(cluster: Cluster): Unit = {
    clusters -= cluster
  }


  def getBrokers: List[Broker] = brokers.toList.sortBy(_.id.toInt)

  def getBroker(id: String) = brokers.find(id == _.id).orNull

  def addBroker(broker: Broker): Broker = {
    if (getBroker(broker.id) != null) throw new IllegalArgumentException(s"duplicate node ${broker.id}")
    brokers += broker
    broker
  }

  def removeBroker(broker: Broker): Unit = { brokers -= broker }

  def reset(): Unit = {
    frameworkId = null

    clear()
  }

  def clear(): Unit = {
    brokers.clear()
    clusters.clear()
  }

  def load() = storage.load()
  def save() = storage.save()

  def fromJson(json: Map[String, Any]): Unit = {
    if (json.contains("clusters")) {
      clusters.clear()
      for (clusterObj <- json("clusters").asInstanceOf[List[Map[String, Object]]])
        addCluster(new Cluster(clusterObj))
    }

    if (json.contains("brokers")) {
      brokers.clear()
      for (brokerNode <- json("brokers").asInstanceOf[List[Map[String, Object]]]) {
        val broker: Broker = new Broker()
        broker.fromJson(brokerNode)
        brokers.append(broker)
      }
    }

    if (json.contains("frameworkId"))
      frameworkId = json("frameworkId").asInstanceOf[String]
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Object]()
    if (frameworkId != null) json("frameworkId") = frameworkId

    if (clusters.nonEmpty) {
      val clustersJson = clusters.map(_.toJson)
      json("clusters") = new JSONArray(clustersJson.toList)
    }

    if (brokers.nonEmpty) {
      val brokersJson = brokers.map(_.toJson())
      json("brokers") = new JSONArray(brokersJson.toList)
    }

    new JSONObject(json.toMap)
  }

  def newStorage(s: String): Storage = {
    if (s.startsWith("file:")) new FsStorage(new File(s.substring("file:".length)))
    else if (s.startsWith("zk:")) new ZkStorage(s.substring("zk:".length))
    else throw new IllegalStateException("Unsupported storage " + s)
  }
}
