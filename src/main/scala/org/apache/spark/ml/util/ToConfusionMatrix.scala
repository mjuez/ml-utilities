package org.apache.spark.ml.util

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object ToConfusionMatrix {

  def toConfusionMatrix(
      predDS: Dataset[Row],
      labelCol: String,
      predictionCol: String
  ): Array[Array[Long]] = {
    val uniqueLabels = predDS.select(col(labelCol)).distinct.collect.map {
      case Row(label: Double) =>
        label
    }

    for(trueLabel <- uniqueLabels) yield {
      for(predictedLabel <- uniqueLabels) yield {
        val rows = predDS
          .filter(
            col(labelCol) === trueLabel && col(predictionCol) === predictedLabel
          )
          .groupBy(col(labelCol))
          .count
          .collect

        if (rows.isEmpty) {
          0
        } else {
          rows.map { case Row(label: Double, count: Long) => count }.head
        }
      }
    }
  }

  def cmToString(cm: Array[Array[Long]]): String = {
    cm.map { arr =>
      arr.mkString("[", ",", "]")
    }.mkString("[", ",", "]")
  }

}
