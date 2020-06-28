import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

//case class emp(Name: String,age: Integer,sal: Integer)
case class Characters(name: String,
                      height: Double,
                      weight: String,
                      eyecolor: Option[String],
                      haircolor: Option[String],
                      jedi: String,
                      species: String)
object Dataset2 {
  def main(args: Array[String]): Unit = {
    val  spark =SparkSession.builder().appName("spark").master("local").getOrCreate()




    //name	gender	height	weight	eyecolor	haircolor	skincolor	homeland	born	died	jedi	species	weapon




  //  Encoders.product[emp]
   //  val men = Encoders[emp]
    import spark.implicits._
    val df =spark.read.option("header","true").option("inferSchema","true").
      csv("C:\\Users\\Shyam\\Desktop\\starwars.csv").as[Characters]
    df.show()
    df.printSchema()

   //val ds = df.as[emp]
   // ds.show()

  }



}
