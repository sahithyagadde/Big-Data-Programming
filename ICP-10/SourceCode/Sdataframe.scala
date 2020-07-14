import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
object Sdataframe{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("My app")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL DataFrames")
      .config(conf =conf)
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    import spark.implicits._
    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/sahithyagadde/Downloads/survey.csv")
    //Save data to the ouput folder
    val save1 = df
      .write.format("com.databricks.spark.csv")
      .save("/Users/sahithyagadde/output1")


    //Apply Union operation on the dataset and order the output by Country Name alphabetically.
    val df1 = df.limit(5)
    val df2 = df.limit(10)
    df1.show()
    df2.show()
    val unionDf = df1.union(df2)
    println("Union Operation : ")
    unionDf.orderBy("Country").show()
    df.createOrReplaceTempView("survey")


    // Check for Duplicate records in the dataset.
    val DupDF = spark.sql("select COUNT(*),Country from survey GROUP By Country Having COUNT(*) > 1")
    println("Duplicate Records : ")
    DupDF.show()


    //Use Group by Query based on treatment
    val treatment = spark.sql("select count(Country) from survey GROUP BY treatment ")
    println("Group by treatment : ")
    treatment.show()

    // Apply the basic queries related to Joins and aggregate functions (at least 2)

    //Aggregate Max and Average
    val MaxDF = spark.sql("select Max(Age) from survey")
    println("Maximum age of a person: ")
    MaxDF.show()
    val AvgDF = spark.sql("select Avg(Age) from survey")
    println("Average age of the people: ")
    AvgDF.show()


    //Join the dataframe using sql
    val df3 = df.select("Country","state","Age","Gender","Timestamp")
    val df4 = df.select("self_employed","treatment","family_history","Timestamp")
    df3.createOrReplaceTempView( "left")
    df4.createOrReplaceTempView("right")
    val joinSQl = spark.sql("select left.Gender,right.treatment,left.state,right.self_employed FROM left,right where left.Timestamp = " +
      "right.Timestamp")
    println("Joining the dataframe: ")
    joinSQl.show(numRows = 50)



    //Write a query to fetch 13th Row in the dataset.
    val df13th = df.take(13).last
    println("13th row in the dataset : ")
    print(df13th)



    //Bonus Question
    // Write a parse Line method to split the comma-delimited row and create a Data frame.

    def parseLine(line: String) =
    {
      val fields = line.split(",")
      val Country = fields(3).toString
      val  state = fields(4).toString
      val  Gender = fields(2).toString
      (Country,state,Gender)
    }
    val lines = sc.textFile("/Users/sahithyagadde/Downloads/survey.csv")
    val rdd = lines.map(parseLine).toDF()
    println("")
    println("ParseLine method : ")
    rdd.show()


  }
}
