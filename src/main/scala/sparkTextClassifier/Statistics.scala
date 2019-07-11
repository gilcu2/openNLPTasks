package sparkTextClassifier

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame

object Statistics {

  def bucketize(df: DataFrame, input: String, output: String): DataFrame = {
    val bucketizer = new Bucketizer()
      .setInputCol("overall")
      .setOutputCol("label")
      .setSplits(Array(Double.NegativeInfinity, 4.0,
        Double.PositiveInfinity))

    bucketizer.transform(df)
  }

}
