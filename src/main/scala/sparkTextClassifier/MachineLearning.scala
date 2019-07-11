package sparkTextClassifier

import org.apache.spark.sql.DataFrame

object MachineLearning {

  def sample(df: DataFrame, column: String, fractions: Map[Double, Double]): DataFrame =
    df.stat.sampleBy(column, fractions, seed = 36L)

}
