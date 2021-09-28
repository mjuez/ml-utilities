package org.apache.spark.ml.util

import scala.util.Random
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object StratifiedCV {

  private type Folds = Array[Dataset[Row]]
  private type RepeatedFolds = Array[Folds]
  private type TTFolds = Array[(Dataset[Row], Dataset[Row])]
  private type RepeatedTTFolds = Array[TTFolds]

  def getStratifiedTrainTestData(
      ds: Dataset[Row],
      reps: Int,
      folds: Int,
      seed: Long
  ): RepeatedTTFolds = {
    makeRepeatedFolds(ds, reps, folds, seed).map { folds =>
      val ids = (0 until folds.length).toArray
      ids.map { testId =>
        val idsBuffer = ids.toBuffer
        idsBuffer.remove(testId)
        val trainFold = idsBuffer
          .map { trainId =>
            folds(trainId)
          }
          .reduce { (res, part) =>
            res.union(part)
          }
        val testFold = folds(testId)
        (trainFold, testFold)
      }
    }
  }

  private def splitDataset(ds: Dataset[Row], folds: Int, seed: Long): Folds = {
    val weight = 1.0 / folds.toDouble
    ds.randomSplit((1 to folds).map { _ => weight }.toArray, seed)
  }

  private def makeRepeatedFolds(
      ds: Dataset[Row],
      reps: Int,
      folds: Int,
      seed: Long
  ): RepeatedFolds = {
    val rnd = new Random(seed)

    val labels = ds
      .groupBy("label")
      .count
      .collect
      .map { r => r.getAs[Double](0) }

    val dsSplits = labels.map { l =>
      ds.filter(col("label") === l)
    } // one dataset per label

    // data set repetitions and stratified folds
    (1 to reps).map { rep =>
      dsSplits
        .map { split =>
          splitDataset(split, folds, rnd.nextLong)
        }
        .reduce { (res, part) =>
          (0 to res.length - 1).map { i =>
            res(i).union(part(i))
          }.toArray
        }
    }.toArray
  }

}
