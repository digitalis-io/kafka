package ly.stealth.mesos.kafka

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

class Cluster {
  var id: String = null
  var zkConnect: String = null

  private[kafka] var topics: Topics = new Topics(() => zkConnect)
  private[kafka] var rebalancer: Rebalancer = new Rebalancer(() => zkConnect)

  def this(_id: String) {
    this
    id = _id
  }

  def this(json: Map[String, Any]) {
    this
    fromJson(json)
  }

  def getBrokers: List[Broker] = Nodes.getBrokers.filter(_.cluster == this)

  def active: Boolean = getBrokers.exists(_.active)
  def idle: Boolean = !active

  def fromJson(json: Map[String, Any]): Unit = {
    id = json("id").asInstanceOf[String]
    if (json.contains("zkConnect"))
      zkConnect = json("zkConnect").asInstanceOf[String]
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()
    json("id") = id
    if (zkConnect != null)
      json("zkConnect") = zkConnect

    new JSONObject(json.toMap)
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Cluster]) false
    else id == obj.asInstanceOf[Cluster].id
  }

  override def toString: String = id
}
