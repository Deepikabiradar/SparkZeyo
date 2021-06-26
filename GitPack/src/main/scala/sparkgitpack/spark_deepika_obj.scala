package sparkgitpack
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.functions._
object spark_deepika_obj {
  
 def main(args:Array[String]):Unit={ 


			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR") 
					
   val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._

	val data = sc.textFile("file:///c:/data/txns")
					
					println("txns data")
					data.take(5).foreach(println)
          
					val fildata = data.filter(x=>x.contains("Gymnastics"))
         
					println("fildata data")
          fildata.take(5).foreach(println)
          fildata.saveAsTextFile("file:///c:/data/gymdata_dir_zeyo")








 }
}
