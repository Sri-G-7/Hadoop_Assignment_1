import org.apache.spark.sql.SparkSession

object ORCTOTEXT {

  def main(args: Array[String]): Unit = {

    print("Welcome")


    // Spark Session object here
    val spark = SparkSession
      .builder
      .appName("Spark training ")
      .master("local[*]")
      .getOrCreate()

    // Reader
    val df=spark.read.option("header", "true").orc("C:\\startups\\part-00000-30019483-aac5-46ca-a117-7fa3a6eb9073-c000.snappy.orc")

    print("=======================")
    print(df.getClass)
    print("=======================")

    df.show(10)
    df.printSchema()

    df.createOrReplaceTempView("welcome_tbl")

    val df2= spark.sql("select * from welcome_tbl limit 10")

    //    df.write.format("csv").mode(SaveMode.Append).save("gs://iwinner-gcs-op-data/startup/")
    df.write.format("text").save("C:\\startups\\ORC_TO_TEXT")


    df2.show(20)



    // Trasnformer



    // Writer


    //    val spark = SparkSession
    //      .builder
    //      .appName("Spark training ")
    //      .master("local[*]").getOrCreate()




  }
}
