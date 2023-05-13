import org.apache.spark.SparkContext

object SparkProgram {
  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val sc = new SparkContext("local[*]", " persistwithstoragelevels")

    // Create RDD
    val rdd = sc.parallelize(1 to 1000000000)


    // Persist RDD with memory-and-disk storage level
   // rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val sum2 = rdd.reduce(_ + _)
    println("Sum of RDD with MEMORY_AND_DISK persisting: " + sum2) //243309312

    // Unpersist RDD from memory
   // rdd.unpersist()
    scala.io.StdIn.readLine()

  }
}
