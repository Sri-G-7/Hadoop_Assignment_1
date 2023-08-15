import org.apache.spark.sql.SparkSession

object CSVTOORC {

  def main(args: Array[String]): Unit = {

    print("Welcome")


    // Spark Session object here
    val spark = SparkSession
      .builder
      .appName("Spark training ")
      .master("local[*]")
      .getOrCreate()

    // Reader
    val df=spark.read.option("header", "true").csv("C:\\startups\\50_Startups.csv")

    print("=======================")
    print(df.getClass)
    print("=======================")

    df.show(10)
    df.printSchema()

    df.createOrReplaceTempView("welcome_tbl")

    val df2= spark.sql("select * from welcome_tbl limit 10")

    //    df.write.format("csv").mode(SaveMode.Append).save("gs://iwinner-gcs-op-data/startup/")
    df.write.format("orc").save("C:\\startups\\CSV_TO_ORC")


    df2.show(20)



    // Trasnformer



    // Writer


    //    val spark = SparkSession
    //      .builder
    //      .appName("Spark training ")
    //      .master("local[*]").getOrCreate()




  }
}
