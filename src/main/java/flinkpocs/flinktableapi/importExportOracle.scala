package flinkpocs.flinktableapi

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.java.io.jdbc._
import java.util.Date

import flinkpocs.flinktableapi.processCsvbatch.aslcc
import org.apache.flink.table.api.{TableEnvironment, Types}
object importExportOracle {
  def main(args: Array[String]) {
    val env=ExecutionEnvironment.getExecutionEnvironment
    val tabEnv=TableEnvironment.getTableEnvironment(env)
    System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true")
    import java.sql.Timestamp
    var inte: TypeInformation[BigInt] = createTypeInformation[BigInt]
    var str: TypeInformation[String] = createTypeInformation[String]
    val dt: TypeInformation[Timestamp] = createTypeInformation[Timestamp]


    val DB_ROWTYPE = new RowTypeInfo(inte,str,str,inte,dt,inte,inte,inte)


    val ds = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("oracle.jdbc.OracleDriver")
      .setDBUrl("""jdbc:oracle:thin://@oracledb.czzw4mblymse.ap-south-1.rds.amazonaws.com:1521/ORCL""")
      .setUsername("ousername")
      .setPassword("opassword")
      .setQuery("SELECT * FROM emp")
      .setRowTypeInfo(DB_ROWTYPE)
      .finish()
    val ds1 =env.createInput(ds)
    ds1.print()


   val input = "C:\\work\\datasets\\asl\\asl.csv"

    // val output = "C:\\work\\datasets\\asl\\result.csv"
    val ds2 :DataSet[aslcc] = env.readCsvFile(input,"\n",",",ignoreFirstLine=true)

    tabEnv.registerDataSet("tab",ds2)
    val res = tabEnv.sqlQuery("select * from tab")
    res.printSchema()

    val jdbcSink=JDBCAppendTableSink.builder()
      .setDrivername("oracle.jdbc.OracleDriver")
      .setDBUrl("jdbc:oracle:thin://@oracledb.czzw4mblymse.ap-south-1.rds.amazonaws.com:1521/ORCL")
      .setUsername("ousername")
      .setPassword("opassword")
      .setQuery("insert into asltab (name,age,city) values(?,?,?)")
      .setParameterTypes(Types.STRING,Types.INT,Types.STRING)
      .build()


    res.writeToSink(jdbcSink)
  //  res.insertInto(jdbcSink)
    env.execute()
  }
}