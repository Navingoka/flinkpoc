package flinkpocs.flinktableapi

import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object wordcountatch {
  def main(args: Array[String]): Unit = {
 //   val env = StreamExecutionEnvironment.getExecutionEnvironment

    val env = ExecutionEnvironment.getExecutionEnvironment
    val path = "C:\\Work\\DataSets\\sample.txt"
    val output ="C:\\Work\\DataSets\\output\\sample.txt"
    val text = env.readTextFile(path) //dataset
    val res = text.flatMap(x=>x.toLowerCase.split("\\W+")).filter(c=>c.nonEmpty)
      .map(x=>(x, 1))
      .groupBy(0)
      .sum(1).setParallelism(1)
    res.print()
    res.writeAsCsv(output)
    //res.writeAsCsv(output)
env.execute()
  }
}