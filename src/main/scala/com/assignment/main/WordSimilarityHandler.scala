package com.assignment.main

/**
  * Created by sdhandapani on 1/25/2017.
  */
object WordSimilarityHandler extends Serializable {

  // Main method invoked from the Spark Shell
  def main(args: Array[String]): Unit = {

    // Check if the input arguments match the expected count of arguments
    if (args.length < 3) {
      println("Usage: WordSimilarityHandler <Input HDFS Path> <Output HDFS Path> <Partition Count>")
      sys.exit(1)
    }

    // Check the format of HDFS path
    if (!args(0).toLowerCase.startsWith("hdfs://") || !args(1).toLowerCase.startsWith("hdfs://")) {
      println("HDFS Path should start with hdfs://. Fix the hdfs path and restart the process.")
      sys.exit(1)
    }

    val inputHdfsPath = args(0)
    val outputHdfsPath = args(1)

    val sc = SparkSingletons.sparkContext
    val rdd = sc.wholeTextFiles(inputHdfsPath, args(3).toInt)
    val simStringBuilder = new StringBuilder("\n")

    //  Parse the document and save the G2Test result at the supplied output HDFS location
    rdd.map { p =>
      val dp = new DocumentParser()
      dp.parseDocument(p._2).wordCombSim.foreach { s =>
        simStringBuilder append f"${s._1} : ${s._2}%.8f" + "\n"
      }
      (p._1, simStringBuilder.toString)
    }.saveAsTextFile(outputHdfsPath)

  }
}
