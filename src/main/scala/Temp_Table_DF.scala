import org.apache.spark.sql.SparkSession

object Temp_Table_DF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Temp Table")
      .master("local")
      .getOrCreate()

    val df = spark.read
        .option("header","true")
      .csv("C:\\Users\\Shyam\\Desktop\\starwars.csv")

    //df.printSchema
    df.createTempView("Customer_Table")
    val limitedCustomer = spark.sql("select * from Customer_Table where haircolor='brown'")
    limitedCustomer.show()

    df.createOrReplaceGlobalTempView("Customer_Table")

    val limitedCustomer2 = spark.sql("select * from Customer_Table limit 10")
    limitedCustomer2.show()
// code
  }

}
