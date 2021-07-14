import $ivy.`org.apache.spark:spark-core_2.12:3.0.0`
import $ivy.`org.apache.spark:spark-sql_2.12:3.0.0`

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

import $ivy.`org.scalanlp::breeze:1.0`
import breeze.linalg._
import breeze.stats.regression.{lasso, leastSquares}

val schema = new StructType()
      .add("likes",DoubleType,true)
      .add("Checkins",DoubleType,true)
      .add("Returns",DoubleType,true)
      .add("Category",DoubleType,true)
      .add("commBase",DoubleType,true)
      .add("comm24",DoubleType,true)
      .add("comm48",DoubleType,true)
      .add("comm24_1",DoubleType,true)
      .add("diff2448",DoubleType,true)
      .add("baseTime",DoubleType,true)
      .add("length",DoubleType,true)
      .add("shares",DoubleType,true)
      .add("hrs",DoubleType,true)
      .add("sun_pub",DoubleType,true)
      .add("mon_pub",DoubleType,true)
      .add("tue_pub",DoubleType,true)
      .add("wed_pub",DoubleType,true)
      .add("thu_pub",DoubleType,true)
      .add("fri_pub",DoubleType,true)
      .add("sat_pub",DoubleType,true)
      .add("sun_base",DoubleType,true)
      .add("mon_base",DoubleType,true)
      .add("tue_base",DoubleType,true)
      .add("wed_base",DoubleType,true)
      .add("thu_base",DoubleType,true)
      .add("fri_base",DoubleType,true)
      .add("sat_base",DoubleType,true)
      .add("output",DoubleType,true)

val conf = new SparkConf().setAppName("Read_CSV").setMaster("local[*]")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
val dfCells = sqlContext.read
                        .format("csv")
                        .schema(schema)
                        .option("header", "true")
                        .option("mode", "DROPMALFORMED")
                        .option("delimiter", ",")
                        .load("datasets/Facebook_Dataset.csv")

println(s"Count of rows before dropna = ${dfCells.count()}")
dfCells.na.drop()
println(s"Count of rows after dropna = ${dfCells.count()}")
dfCells.dropDuplicates()
println(s"Count of rows after dropDuplicates = ${dfCells.count()}")

val value_result_column:String = "output"
val collection_dep_columns = dfCells.select(value_result_column).rdd.collect
val dep = DenseVector(collection_dep_columns.map(_.getDouble(0)):_*)

val collection_indep_column = dfCells.drop(value_result_column).rdd.collect
val indep = DenseMatrix(collection_indep_column.map(_.toSeq.asInstanceOf[Seq[Double]]):_*)


