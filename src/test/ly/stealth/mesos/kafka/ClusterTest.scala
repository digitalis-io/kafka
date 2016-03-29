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

import org.junit.{Before, Test}
import org.junit.Assert._

class ClusterTest extends KafkaMesosTestCase {

  @Before
  override def before {
    super.before
    Nodes.clear()
  }

  @Test
  def addCluster_removeCluster {
    assertTrue(Nodes.getBrokers.isEmpty)

    val cluster0 = Nodes.addCluster(new Cluster("0"))
    val cluster1 = Nodes.addCluster(new Cluster("1"))
    assertEquals(Set(cluster0, cluster1), Nodes.getClusters.toSet)

    Nodes.removeCluster(cluster0)
    assertEquals(Set(cluster1), Nodes.getClusters.toSet)

    Nodes.removeCluster(cluster1)
    assertTrue(Nodes.getClusters.isEmpty)
  }

  @Test
  def getCluster {
    assertNull(Nodes.getCluster("0"))

    val broker0 = Nodes.addCluster(new Cluster("0"))
    assertSame(broker0, Nodes.getCluster("0"))
  }

  @Test
  def save_load {
    Nodes.addCluster(new Cluster("0"))
    Nodes.addCluster(new Cluster("1"))
    Nodes.save()

    Nodes.load()
    assertEquals(2, Nodes.getClusters.size)
  }

  @Test
  def toJson_fromJson {
    val cluster = Nodes.addCluster(new Cluster("0"))
    cluster.zkConnect = "zk://master:2181"

    val read: Cluster = new Cluster()
    read.fromJson(Util.parseJson("" + cluster.toJson))

    assertEquals(cluster.id, read.id)
    assertEquals(cluster.zkConnect, read.zkConnect)
  }
}
