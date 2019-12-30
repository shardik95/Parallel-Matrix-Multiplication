package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.SparkSession

object Matrix {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Usage:\nexample.Matrix <input dir> <output dir>")
      System.exit(1)
    }

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================
    val conf = new SparkConf().setAppName("Generate example Matrix")
    val sc = new SparkContext(conf)
    val p = args(0)
    val q = args(1)
    val r = args(2)
    val output= args(3)


    def generateMatrix(sc:SparkContext,p:String,q:String, outputPath:String,matrixId:String) {
      val matrixL = RandomRDDs.uniformVectorRDD(sc, p.toInt, q.toInt)

      val stringRDD = matrixL.zipWithIndex().map {
        case (vec, id) => {
          var v = matrixId+"," + (id + 1).toString
          for (i <- 0 until q.toInt) v = v + "," + (i + 1).toString + ":" + (100 * vec.apply(i)).toInt.toString
          v
        }
      }


      stringRDD.saveAsTextFile(outputPath)
    }
    generateMatrix(sc,p,q,output+"/leftMatrix","L")
    generateMatrix(sc,q,r,output+"/rightMatrix","R")

  }
}
