import org.apache.spark._
import org.apache.spark.streaming._

 // not necessary since Spark 1.3
object Streaming {


   def main(args: Array[String]): Unit = {

     // Create a local StreamingContext with two working thread and batch interval of 1 second.
     // The master requires 2 cores to prevent a starvation scenario.
     val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
     conf.set("spark.driver.host", "127.0.0.1");
     val sc = new SparkContext(conf)
     val ssc = new StreamingContext(sc, Seconds(10))

     // Create a DStream that will connect to hostname:port, like localhost:9999
     val lines = ssc.socketTextStream("localhost", 9999)


     // Split each line into words
     val words = lines.flatMap(_.split(" "))
     val pairs = words.map(word => (word, 1))
     val wordCounts = pairs.reduceByKey(_ + _)

     // Print the first ten elements of each RDD generated in this DStream to the console
     wordCounts.print()

     ssc.start() // Start the computation
     ssc.awaitTermination() // Wait for the computation to terminate
   }
}
