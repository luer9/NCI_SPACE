import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object Configuration {
  val sparkSession = loadSparkSession(loadSparkConf())
  val sparkContext = loadSparkContext(sparkSession)
  var inputTriFile = ""
  var inputPredFile = ""
  var NCIDir = ""
  var outputDIR = ""
  var originFile = ""
  def loadUserSettings(
//                        originfile: String,
//                        inputTriFile: String,
//                        inputPredFile: String,
                        NCIDIR: String,
                        outputDIR: String) = {
//    this.inputTriFile = inputTriFile
//    this.inputPredFile = inputPredFile
    this.outputDIR = outputDIR
    this.NCIDir = NCIDIR
//    this.originFile = originfile
  }


  /**
   * Create SparkContext.
   * The overview over settings:
   * http://spark.apache.org/docs/latest/programming-guide.html
   */
  def loadSparkConf(): SparkConf = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("NCI_SPACE")
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions", "8")
      .set("spark.default.parallelism", "12")
      .set("spark.driver.memory", "15G")
      .set("spark.executor.memory", "20G")
    conf
  }

  def loadSparkSession(conf: SparkConf): SparkSession  = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark
  }
  def loadSparkContext(sparkSession: SparkSession): SparkContext = {
    sparkSession.sparkContext
  }
}
