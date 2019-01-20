package flinkpocs.flinktableapi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object processCsvbatch {
  case class aslcc(name:String, age:Int, city:String)
  case class na(name:String,age:Int)
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    //spark rdd, dataframe , dataset
    //flink dataset, datastream

    val path = "C:\\Work\\DataSets\\asl.txt"
    val op = "C:\\Work\\DataSets\\output\\asl.txt"
    val asl:DataSet[aslcc] = env.readCsvFile(path,"\n",",",ignoreFirstLine = true)
tEnv.registerDataSet("asl",asl)

    val res = tEnv.sqlQuery("select * from asl where city='blr'").toDataSet[aslcc]
    val mas = tEnv.sqlQuery("select name, age from asl where city='mas'").toDataSet[na]
    res.writeAsCsv(op)
    env.execute()
  }
}