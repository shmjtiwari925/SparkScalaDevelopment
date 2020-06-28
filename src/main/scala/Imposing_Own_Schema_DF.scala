
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Imposing_Own_Schema_DF {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Imposing own schema")
      .master("local")
      .getOrCreate()

    val df = spark.read.option("header","true")
      .option("infraSchema","true")
      .csv("C:\\Users\\Shyam\\Desktop\\test.csv")

   // df.show()
    df.printSchema()

    val OwnSchema = StructType(
      StructField("Name",StringType,true)::
        StructField("age", IntegerType,false)::
        StructField("sal",StringType,true)::Nil)

    val df2 = spark.read.option("header","true")
      .schema(OwnSchema)
      .csv("C:\\Users\\Shyam\\Desktop\\test.csv")
    df2.printSchema()



  }

}
