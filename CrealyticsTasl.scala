// I used the Apache spark dataframe for the implementation along with scala. I am not very proficient in scala as i mostly work with pyton and java. As this task is quite simple so i implemented with spark, scala , jupyter, Apache torre as Scala kernel. I ran this code in spark-shell and in apache torre scala kernel with spark. 

// Total time to finish = Approximately 30 minitues. 


import scala.io.Source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import spark.implicits._
object CrealyticsTask{
    def main(args: Array[String]){
        val filename=args(0) //take the first argument as input file
        val spark = SparkSession //initialize sparksession, not necessary if it runs from spark-shell or spark-submit
          .builder()
          .appName("Spark SQL basic example")
          .config("spark.some.config.option", "some-value")
          .getOrCreate()
        //now read the csv with spark sql dataframe
        val df1 = spark.read.option("header","false") 
                        .option("delimiter","\t")
                        .option("inferSchema", "true")
                        .csv(filename)
                        .toDF("times","values")
        // take 60 as window sliding value order by timestap
        // take Window functions from Spark Sql dataframe
        val windowSpec = Window.orderBy(functions.col("times")).rangeBetween(-60,0)
        // import spark sql dataframe functions and implement the required functions like count, sum, min or max
        val df2 = (df1.withColumn("N_O",functions.count("values").over(windowSpec))
                   .withColumn("Roll_sum", functions.sum("values").over(windowSpec))
                   .withColumn("Min_Value", functions.min("values").over(windowSpec))
                   .withColumn("Max_Value", functions.max("values").over(windowSpec)))
        //write the result in a text file or in shell , view the result with .view() command
        val op= df2.write.mode("overwrite").format("csv").option("header", "true")
    .option("inferSchema", "true").save("/media/hasan/ec42e2af-0eaa-4b8c-81d5-f2a23ec16363/Downloads/Trail-Task/timeseries/result")
  // your scala code here

        
    }
}
