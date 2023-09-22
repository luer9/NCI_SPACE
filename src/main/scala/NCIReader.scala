import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object NCIReader {

  val spark: SparkSession = Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext

  import spark.implicits._

  def IndexReader(
                   path: File
                 ): (String, String, Int, DataFrame) = {

      val infor = path.getName.split("_")
      val pp = infor.apply(0) // kc id
      val cir = infor.apply(1) // circle or not
      val len = infor.apply(2).toInt // max len
      val curNCIDF = spark.sqlContext.read.parquet(path + File.separator + "NCI").toDF()
      println("--[pp] " + pp)
      println("--[cir] " + cir)
      println("--[max len] " + len)
      println("--[processing...]" )
      (pp, cir, len, curNCIDF)

  }

  def saveNCI(res: DataFrame, len: Long, cir: Boolean, pp: String, exper: Boolean = false): Unit = {
    if (!exper) {
      res.select("value").write.parquet(Configuration.outputDIR + File.separator
        + pp + "_" + cir + "_" + len + File.separator + "NCI")
    } else {
      res.select("value").write.parquet(Configuration.outputDIR + File.separator
        + pp + "-" + cir + "-" + len + File.separator + "NCI")
    }

    println("|------[Path] " + res.count())
  }

}
