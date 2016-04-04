package ly.stealth.mesos.kafka

import java.io.IOException
import java.util
import java.util.Collections
import Cli._
import joptsimple.{OptionException, OptionSet}

object ClusterCli {
  def handle(cmd: String, _args: Array[String], help: Boolean = false): Unit = {
    var args = _args

    if (help) {
      handleHelp(cmd)
      return
    }

    var arg: String = null
    if (args.length > 0 && !args(0).startsWith("-")) {
      arg = args(0)
      args = args.slice(1, args.length)
    }

    if (arg == null && cmd != "list") {
      handleHelp(cmd); printLine()
      throw new Error("argument required")
    }

    cmd match {
      case "list" => handleList(arg)
      case "add" | "update" => handleAddUpdate(arg, args, cmd == "add")
      case "remove" => handleRemove(arg)
      case _ => throw new Error("unsupported broker command " + cmd)
    }
  }

  private[kafka] def printCmds(): Unit = {
    printLine("Commands:")
    printLine("add          - add cluster", 1)
    printLine("update       - update cluster configuration", 1)
    printLine("remove       - remove cluster", 1)
    printLine("list         - list existing clusters", 1)
  }

  private def handleHelp(cmd: String): Unit = {
    cmd match {
      case null =>
        printLine("Cluster management commands\nUsage: cluster <command>\n")
        ClusterCli.printCmds()

        printLine()
        printLine("Run `help cluster <command>` to see details of specific command")
      case "list" =>
        handleList(null, help = true)
      case "add" | "update" =>
        handleAddUpdate(null, null, cmd == "add", help = true)
      case "remove" =>
        handleRemove(null, help = true)
      case _ =>
        throw new Error(s"unsupported cluster command $cmd")
    }
  }

  private def handleList(expr: String, help: Boolean = false): Unit = {
    if (help) {
      printLine("List clusters\nUsage: cluster list\n")
      handleGenericOptions(null, help = true)

      printLine()

      return
    }

    var json: Map[String, Object] = null
    try { json = sendRequest("/cluster/list", new util.HashMap[String, String]) }
    catch { case e: IOException => throw new Error("" + e) }

    val clusters = json("clusters").asInstanceOf[List[Map[String, Object]]]
    val title = if (clusters.isEmpty) "no clusters" else "cluster" + (if (clusters.size > 1) "s" else "") + ":"
    printLine(title)

    for (clusterJson <- clusters) {
      val cluster = new Cluster()
      cluster.fromJson(clusterJson)

      printCluster(cluster, 1)
      printLine()
    }
  }

  private def printCluster(cluster: Cluster, indent: Int): Unit = {
    printLine("id: " + cluster.id, indent)
    printLine("zk connection string: " + cluster.zkConnect, indent)
    printLine("controller: " + (if (cluster == null) "<auto>" else cluster.controller), indent)
  }

  private def handleAddUpdate(id: String, args: Array[String], add: Boolean, help: Boolean = false): Unit = {
    val parser = newParser()
    parser.accepts("zk-connect", "REQUIRED. Connection string to Kafka Zookeeper cluster. E.g.: 192.168.0.1:2181,192.168.0.2:2181/kafka1")
      .withRequiredArg().ofType(classOf[String])
    parser.accepts("controller", "Optionally pins controller to a broker.")
      .withRequiredArg().ofType(classOf[java.lang.Integer])

    if (help) {
      val cmd = if (add) "add" else "update"
      printLine(s"${cmd.capitalize} cluster\nUsage: cluster $cmd <cluster-id> [options]\n")
      parser.printHelpOn(out)

      printLine()
      handleGenericOptions(null, help = true)

      if (!add) printLine("\nNote: use \"\" arg to unset an option")
      return
    }

    var options: OptionSet = null
    try {
      options = parser.parse(args: _*)
    } catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
        throw new Error(e.getMessage)
    }

    val zkConnect = options.valueOf("zk-connect").asInstanceOf[String]
    val controller = options.valueOf("controller").asInstanceOf[java.lang.Integer]

    val params = new util.LinkedHashMap[String, String]
    params.put("cluster", id)

    if (zkConnect != null) params.put("zkConnect", zkConnect)
    if (controller != null) params.put("controller", controller.toString)

    var json: Map[String, Object] = null
    try {
      json = sendRequest("/cluster/" + (if (add) "add" else "update"), params)
    } catch {
      case e: IOException => throw new Error("" + e)
    }

    val clusters = json("clusters").asInstanceOf[List[Map[String, Object]]]
    val op = if (add) "added" else "updated"
    printLine(s"cluster $op:")
    for (clusterJson <- clusters) {
      val cluster = new Cluster(clusterJson)

      printCluster(cluster, 1)
      printLine()
    }
  }

  private def handleRemove(id: String, help: Boolean = false): Unit = {
    if (help) {
      printLine("Remove cluster\nUsage: cluster remove <cluster-id>\n")
      handleGenericOptions(null, help = true)

      return
    }

    var json: Map[String, Object] = null
    try { json = sendRequest("/cluster/remove", Collections.singletonMap("cluster", id)) }
    catch { case e: IOException => throw new Error("" + e) }

    val resId = json("id").asInstanceOf[String]

    printLine(s"cluster $resId removed")
  }
}
