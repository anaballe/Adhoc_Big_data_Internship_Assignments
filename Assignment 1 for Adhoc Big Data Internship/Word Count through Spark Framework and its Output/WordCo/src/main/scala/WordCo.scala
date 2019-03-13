import scala.math.random
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
 object WordCo {
 	def main(args: Array[String]) {
 		val conf = new SparkConf().setAppName("Word Count")
 		val spark = new SparkContext(conf)
 		val a = spark.textFile("/root/Desktop/test_data.txt")
 		
 		val c = a.flatMap(lines => lines.split("[ ,]")).map(wc => (wc,1)).reduceByKey(_+_)
 		c.foreach(println)
 		c.saveAsTextFile("/root/Desktop/ResultsforWordCount")
 		
 		
 	}
 	
 }