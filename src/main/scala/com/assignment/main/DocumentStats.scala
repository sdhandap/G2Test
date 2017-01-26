package com.assignment.main

/**
  * Created by sdhandapani on 1/25/2017.
  */

case class DocumentStats(sentenceCount: Int,
                         wordFrequencyMap: Map[String, Int],
                         wordCombFrequencyMap: Map[(String, String), Int],
                         wordCombSim: Array[((String, String), Double)]
                        )
