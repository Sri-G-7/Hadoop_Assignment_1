import org.apache.spark.sql.SparkSession

object PARQUETTOORC {

  def main(args: Array[String]): Unit = {

    print("Welcome")


    // Spark Session object here
    val spark = SparkSession
      .builder
      .appName("Spark training ")
      .master("local[*]")
      .getOrCreate()

    // Reader
    val df=spark.read.option("header", "true").parquet("C:\\startups\\part-00000-2291c4ab-b88c-4e00-a98e-2954a7e6b84c-c000.snappy.parquet")

    print("=======================")
    print(df.getClass)
    print("=======================")

    df.show(10)
    df.printSchema()

    df.createOrReplaceTempView("welcome_tbl")

    val df2= spark.sql("select * from welcome_tbl limit 10")

    //    df.write.format("csv").mode(SaveMode.Append).save("gs://iwinner-gcs-op-data/startup/")
    df.write.format("orc").save("C:\\startups\\PARQUET_TO_ORC")


    df2.show(20)



    // Trasnformer



    // Writer


    //    val spark = SparkSession
    //      .builder
    //      .appName("Spark training ")
    //      .master("local[*]").getOrCreate()




  }
}
