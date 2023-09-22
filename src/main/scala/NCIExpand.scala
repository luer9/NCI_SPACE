import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import java.io.File

object NCIExpand {

  val spark: SparkSession = Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext

  import spark.implicits._

  def Main(
            NCIDIR: String
          ): Unit = {
    val paths = new File(NCIDIR)

    for (path <- subdirs(paths)) {

      val schema1 = StructType(
        Seq(
          StructField("value", StringType, true)
        ))
      var finalPath = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
      var emptySeq: Seq[Row] = Seq.empty[Row]


      val NCIInfor = NCIReader.IndexReader(path)
      NCIInfor._4.collect().map(row => {
        val arr = row.mkString.split(",")
        for( i <- 0 to arr.length - 2) {
          emptySeq = emptySeq :+ Row(arr.apply(i) + "," + arr.drop(i + 1).mkString(","))
//          println(arr.apply(i) + " , " + arr.drop(i + 1).mkString("_"))
        }
      })
      finalPath = spark.createDataFrame(spark.sparkContext.parallelize(emptySeq), schema1)
      NCIReader.saveNCI(finalPath, NCIInfor._3, NCIInfor._2.toBoolean, NCIInfor._1)
    }
  }

  def subdirs(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter(_.isDirectory())
    children.toIterator
    //    ++ children.toIterator.flatMap(subdirs _)
  }

}