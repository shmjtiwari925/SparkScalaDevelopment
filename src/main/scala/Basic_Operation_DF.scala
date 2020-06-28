import org.apache.spark.sql.SparkSession

object Basic_Operation_DF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Basic OperationDF")
      .master("local")
      .getOrCreate()


    val df = spark.read.csv("C:\\Users\\Shyam\\Desktop\\starwars.csv")
     val CustSchema = df.schema
    println(CustSchema)
    val CustColumn = df.columns
    println("Column Names")
    println(CustColumn mkString(" , "))
     println("code")
    //al Columninfo = df.describe("_c0")
    //println(Columninfo)
    //df.head(5).foreach(println)
    //df.show()
     //df.select("_c0","_c4").show()
    //val filterdf = df.select("_c0","_c1").where("_c4=='brown'")
    val filterdf = df.select("_c0","_c4")
        //.filter(df("_c4")==="brown")
        .groupBy("_c4").count()
    filterdf.show()



  }

}
