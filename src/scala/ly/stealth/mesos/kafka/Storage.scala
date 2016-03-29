package ly.stealth.mesos.kafka

import java.io.{FileWriter, File}

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException

trait Storage {
  def load(): Unit = {
    val json: String = loadJson
    if (json == null) return

    val node: Map[String, Object] = Util.parseJson(json)
    Nodes.fromJson(node)
  }

  def save(): Unit = {
    saveJson("" + Nodes.toJson)
  }

  protected def loadJson: String
  protected def saveJson(json: String): Unit
}

class FsStorage(val file: File) extends Storage {
  protected def loadJson: String = {
    if (!file.exists) return null
    val source = scala.io.Source.fromFile(file)
    try source.mkString finally source.close()
  }

  protected def saveJson(json: String): Unit = {
    val writer  = new FileWriter(file)
    try { writer.write(json) }
    finally { writer.close() }
  }
}

object FsStorage {
  val DEFAULT_FILE: File = new File("kafka-mesos.json")
}

class ZkStorage(val zk: String) extends Storage {
  val (zkConnect, path) = zk.span(_ != '/')
  createChrootIfRequired()

  def zkClient: ZkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)

  private def createChrootIfRequired(): Unit = {
    if (path != "") {
      try { zkClient.createPersistent(path, true) }
      finally { zkClient.close() }
    }
  }

  protected def loadJson: String = {
    val zkClient = this.zkClient
    try { zkClient.readData(path, true).asInstanceOf[String] }
    finally { zkClient.close() }
  }

  protected def saveJson(json: String): Unit = {
    val zkClient = this.zkClient
    try { zkClient.createPersistent(path, json) }
    catch { case e: ZkNodeExistsException => zkClient.writeData(path, json) }
    finally { zkClient.close() }
  }
}
