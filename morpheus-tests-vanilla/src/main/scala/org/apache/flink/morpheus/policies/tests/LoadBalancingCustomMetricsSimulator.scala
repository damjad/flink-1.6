package org.apache.flink.morpheus.policies.tests

import org.apache.flink.morpheus.generators.CustomDistSentenceGenerator
import org.apache.flink.morpheus.generators.utils.dists.{LinearWeightedUniformDistribution, RoundRobinDistribution, UniformDistribution}
import org.apache.flink.runtime.morpheus.LoadBalancingPolicyCustomMetrics

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LoadBalancingCustomMetricsSimulator {

  val parallelism = 5
  val vNodesPerOp = 5
  val maxParallelism: Int = parallelism * vNodesPerOp * 16
  val wordLength = 3
  val wordListSize: Int = Math.pow(26, 3).toInt

  def main(args: Array[String]): Unit = {

    val opUniform = new UniformDistribution(parallelism)
    val opWeightedUniform = new LinearWeightedUniformDistribution(10, 1, parallelism)

    val gen = new CustomDistSentenceGenerator(1000000,
      100,
      wordLength,
      maxParallelism,
      opWeightedUniform,
      new RoundRobinDistribution(vNodesPerOp),
      new UniformDistribution(wordListSize)
    )



    var costBuffer = ArrayBuffer[(Double, Int)]()

    for (t <- 0.05d to 1d by 0.05d) {

      val vNodesTrack = mutable.HashMap[Int, mutable.Buffer[Int]]()
      var tCountPerInst = mutable.HashMap[Int, Int]()
      var tCountPerVNode = mutable.HashMap[Int, Int]()

      for (i <- Range(0, parallelism * vNodesPerOp)) {
        vNodesTrack.getOrElseUpdate(i % parallelism, new ArrayBuffer[Int]()).append(i)
      }

      println(t)
      for (i <- Range(0, 10)) {
        for (j <- Range(0, 100000)) {
          val randOpId = gen.getNextOpId
          val randVNodeId = gen.getNextVNodeId(randOpId)
          val nOpId = vNodesTrack.filter(x => x._2.contains(randVNodeId)).head._1
          tCountPerInst.getOrElseUpdate(nOpId, 0)
          tCountPerInst(nOpId) += 1

          tCountPerVNode.getOrElseUpdate(randVNodeId, 0)
          tCountPerVNode(randVNodeId) += 1
        }

        val movements = LoadBalancingPolicyCustomMetrics.calculateMovement(t, tCountPerInst, tCountPerVNode, vNodesTrack)

        if (movements.nonEmpty) {
          println()
          println(s"stage $i:")
          println("counts per instance")
          println(tCountPerInst.toList.sortBy(_._1))

          costBuffer.append((t, meanDistance(tCountPerInst.values.toSeq)))

          tCountPerInst = mutable.HashMap[Int, Int]()
          tCountPerVNode = mutable.HashMap[Int, Int]()
          movements.foreach(x => println(x._1, x._2, "::", x._3.mkString(",")))
        }
      }
    }

    println()
    println(costBuffer.sortBy(_._2))
    println()
    println(costBuffer.groupBy(_._1).map(x => (x._1, x._2.size, x._2)).toList.sortBy(_._2))
    println()
    println(costBuffer.groupBy(_._1).map(x => (x._1, x._2.size, x._2)).toList.sortBy(_._1))
  }


  def meanDistance(s: Seq[Int]) = {
    val mean = s.sum / s.size
    s.map(x => Math.abs(mean - x)).sum / s.size
  }

}
