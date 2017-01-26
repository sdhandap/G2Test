package com.assignment.main

/**
  * Created by sdhandapani on 1/25/2017.
  */
object WordSimilarityHandler extends Serializable {

  // Main method invoked from the Spark Shell
  def main(args: Array[String]): Unit = {

    // Check if the HDFS path is supplied. If not, error out stating missing HDFS Path
    if (args.length < 2) {
      println("Usage: WordSimilarityHandler <Input HDFS Path> <Output HDFS Path>")
      sys.exit(1)
    }

    if (!args(0).toLowerCase.startsWith("hdfs://") || !args(1).toLowerCase.startsWith("hdfs://")) {
      println("HDFS Path should start with hdfs://. Fix the hdfs path and restart the process.")
      sys.exit(1)
    }

    val inputHdfsPath = args(0)
    val outputHdfsPath = args(1)

    val sc = SparkSingletons.sparkContext
    val rdd = sc.wholeTextFiles(inputHdfsPath, 4)
    val simStringBuilder = new StringBuilder("\n")


    rdd.map { p =>
      val dp = new DocumentParser()
      dp.parseDocument(p._2).wordCombSim.foreach { s =>
        simStringBuilder append f"${s._1} : ${s._2}%.8f" + "\n"
      }
      (p._1, simStringBuilder.toString)
    }.saveAsTextFile(outputHdfsPath)

  }
}
