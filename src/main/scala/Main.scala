object Main {

  def main(args: Array[String]): Unit = {

    val NCIDIR = "F:\\dbproject\\NCI_INDEX\\RESULT\\UOBM20"
    val outputDIR = "F:\\dbproject\\NCI_SPACE\\RESULT\\UOBM20"
    Configuration.loadUserSettings(NCIDIR, outputDIR)
    val st = System.currentTimeMillis()
    NCIExpand.Main(NCIDIR)
    val end = System.currentTimeMillis()
    println("[DONE] " + (end - st) + "ms")
  }
}
