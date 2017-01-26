package com.assignment.main

import scala.collection.mutable

/**
  * Created by sdhandapani on 1/25/2017.
  */

class DocumentParser extends Serializable {

  /*
    Main method that will accept contents of a document, parses it and determines word similarity within context of a sentence.
   */
  def parseDocument(fileContent: String): DocumentStats = {
    // Capture frequency of word combination within a context
    val wordCombFrequencyMap: mutable.HashMap[(String, String), Int] = mutable.HashMap()
    // Capture frequency of word within a context
    val wordFrequencyMap: mutable.HashMap[String, Int] = mutable.HashMap()
    var sentenceCount: Int = 0
    val sentenceWordSet: mutable.HashSet[String] = mutable.HashSet()
    var filePointer = 0
    var sentenceCompleted: Boolean = false

    while (filePointer < fileContent.length) {
      val wordStartPos = filePointer
      var wordEndPos = wordStartPos
      while (fileContent(filePointer) != ' ' && fileContent(filePointer) != '\t' &&
        fileContent(filePointer) != '\n' && fileContent(filePointer) != '.') {
        wordEndPos += 1
        filePointer += 1
      }

      if (fileContent(filePointer) == '.')
        sentenceCompleted = true

      val word = removeSpecialChars(fileContent.substring(wordStartPos, wordEndPos).toLowerCase.trim)
      if (word != "")
        sentenceWordSet update(word, true)

      if (sentenceCompleted || filePointer == fileContent.length - 1) {
        sentenceCount += 1
        val result = wordCombination(sentenceWordSet)
        //    println(result)
        updateWordCombinationFrequency(result, wordCombFrequencyMap)
        updateWordFrequency(sentenceWordSet, wordFrequencyMap)
        sentenceWordSet.clear()
        sentenceCompleted = false
      }
      filePointer += 1
    }

    println(s"Number of sentences in the document: $sentenceCount")
    val wordSimilarity = computeSimilarity(wordCombFrequencyMap, wordFrequencyMap, sentenceCount)
    DocumentStats(sentenceCount, wordFrequencyMap.toMap, wordCombFrequencyMap.toMap, wordSimilarity)
  }

  /*
  Update the word frequency map with set of words found in each sentence.
   */
  private def updateWordFrequency(words: mutable.Set[String],
                                  wordFrequencyMap: mutable.HashMap[String, Int]): Unit = {
    for (word <- words) {
      util.Try(wordFrequencyMap(word)) match {
        case util.Success(s) => wordFrequencyMap update(word, s + 1)
        case util.Failure(err) => wordFrequencyMap put(word, 1)
      }
    }
  }

  /*
  Update word combination frequency map with set of word combination identified in each sentence.
   */
  private def updateWordCombinationFrequency(wordComb: mutable.Set[(String, String)],
                                             wordCombFrequencyMap: mutable.HashMap[(String, String), Int]): Unit = {
    for (elem <- wordComb) {
      util.Try(wordCombFrequencyMap(elem)) match {
        case util.Success(s) => wordCombFrequencyMap update(elem, s + 1)
        case util.Failure(err) => wordCombFrequencyMap put(elem, 1)
      }
    }
  }

  // Remove special characters in the input word
  private def removeSpecialChars(str: String): String = {
    str.replaceAll("[\\W]", "")
  }

  /*
  Create possible combination of words from set of words. Sort the elements first, so the created combination
  has the words in ascending order. First element of the tuple is always less than second element.
   */
  private def wordCombination(ws: mutable.HashSet[String]) = {
    val sortedWs = ws.toSeq.sorted // Need to see if there is a way to optimize this, instead of external sorting
    val wordSetComb: mutable.TreeSet[(String, String)] = mutable.TreeSet()
    var wsTail = sortedWs
    for (i <- sortedWs) {
      wsTail = wsTail.tail
      for (j <- wsTail) {
        wordSetComb update((i, j), true)
      }
    }
    wordSetComb
  }

  /*
  Compute the similarity using the supplied formula p(X,Y) log ( p(X,Y) / p(X) * p(Y))
   */
  private def computeSimilarity(wordCombFreqMap: mutable.HashMap[(String, String), Int],
                                wordFreqMap: mutable.HashMap[String, Int],
                                contextSize: Int): Array[((String, String), Double)] = {

    val wordCombSim: Array[((String, String), Double)] = Array.ofDim[((String, String), Double)](wordCombFreqMap.size)
    var idx = 0
    println(s"Context Size: $contextSize")
    wordCombFreqMap.foreach { wordComb =>
      val word1Prob: Double = wordFreqMap(wordComb._1._1) / contextSize.toDouble
      val word2Prob: Double = wordFreqMap(wordComb._1._2) / contextSize.toDouble
      val word1n2Prob: Double = wordComb._2 / contextSize.toDouble
      val logResult = Math.log(word1n2Prob / (word1Prob * word2Prob))
      val wordCombSimResult = (wordComb._1, word1n2Prob * logResult)
      wordCombSim(idx) = wordCombSimResult
      idx += 1
    }
    wordCombSim
  }

}
