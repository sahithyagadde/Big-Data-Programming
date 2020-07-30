import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
object task2 {
  def main(args: Array[String]): Unit = {
    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Task2")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Task2")
      .config(conf =conf)
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // We are using all 3 fifa dataset given on Kaggle Repository
    //a.Import the dataset and create df and print Schema
    val df1 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("fifa-world-cup/WorldCups.csv")
    val df2 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("fifa-world-cup/WorldCupPlayers.csv")
    val df3 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("fifa-world-cup/WorldCupMatches.csv")

    // Printing the Schema
    df1.printSchema()
    df2.printSchema()
    df3.printSchema()

    //b.Perform   10   intuitive   questions   in   Dataset
    //For this problem we have used the Spark SqL on DataFrames
    //First of all create three Temp View
    df1.createOrReplaceTempView("WC")
    df2.createOrReplaceTempView("Players")
    df3.createOrReplaceTempView("Matches")

    // Display the names of the winner along with the name of the country and the year in which they have won.
    val Query = spark.sql("select Winner, Country, Year from WC Order By Country ")
    Query.show()

    //Display the number of teams qualified and the matches played by the country BRAZIL.
    val Query1 = spark.sql("select QualifiedTeams, MatchesPlayed, Year from WC WHERE Country = 'Brazil' Order By Year")
    Query1.show()

    //Display the cities that hosted highest world cup matches.
    val Query2 = spark.sql("select Count(City),City from Matches Group By City")
    Query2.show()

    //Display the teams with the most world cup final victories
    val Query3 = spark.sql("select Count(Winner),Winner,Attendance from WC Group By Winner, Attendance")
    Query3.show()

    // Display the Finalers in the year 1934
    val Query4 = spark.sql("select * from Matches where Stage='Final' AND Year  = 1934 ")
    Query4.show()

    //Display the matches which are held by the coach CAUDRON Raoul (FRA)
    val Query5 =spark.sql("select * from Players where `Coach Name` = 'CAUDRON Raoul (FRA)'")
    Query5.show()

    //Display the number of matches which are held in the san siro stadium in the year 1934.
    val Query6 = spark.sql("select count(*) from Matches where year=1934 AND Stadium = 'San Siro' ")
    Query6.show()

    //Display the total number of matches which are held in Estadio Centenario stadium
    val Query7 = spark.sql("select count(*) from Matches where Stadium = 'Estadio Centenario'")
    Query7.show()

    //Display the country and the count of the number times world cup is hosted by each country.
    val Query8 = spark.sql("select Count(Country),Country,Year from WC Group by Country,Year")
    Query8.show()

    //Display the Stadium and the number of times world cup is hosted.
    val Query9 = spark.sql("select Count(Stadium),Stadium from Matches Group By Stadium")
    Query9.show()

    //Display the name of the player where position= GK
    val Query10 = spark.sql("select `Player Name`, Position from Players where Position = 'GK' ")
    Query10.show()

    //Display the Name of the home team, year and their stage by Years
    val Query11 = spark.sql("select `Home Team Name`,Stage,Year FROM Matches Group By Year,`Home Team Name`,Stage")
    Query11.show()

    // Display the Away Team Name and their stage
    val Query12 = spark.sql("select `Away Team Name`,Stage,Year from Matches Group By Year,`Away Team Name`,Stage")
    Query12.show()


    //Perform any 5 queries in Spark RDD’s and Spark Data Frames.
    // To Solve this Problem we first create the rdd as we already have Dataframe df1 created above code
    // RDD creation

    val csv = sc.textFile("fifa-world-cup/WorldCups.csv")

    val h1 = csv.first()

    val data = csv.filter(line => line != h1)

    data.foreach(println)

    val rdd = data.map(line=>line.split(",")).collect()

    //rdd.foreach(println)
    //RDD Highest Numbers of goals
    val rdd1 = data.filter(line => line.split(",")(0) == "2006").map(line => (line.split(",")(0),
      (line.split(",")(1)), (line.split(",")(2)), (line.split(",")(3)) ) )
    rdd1.foreach(println)

    // Dataframe
    df1.select("Year","Country", "Winner").filter("Year =2006").show(10)

    // Dataframe SQL
    val dfQ1 = spark.sql("select Year, Country, Winner FROM WC WHERE Year = 2006 order by Year Desc Limit 10").show()

    // Year, Venue country = winning country
    // Using RDD
    val rdd2 = data.filter(line => (line.split(",")(2)=="Italy" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3),line.split(",")(4),line.split(",")(5)
        ,line.split(",")(5))).collect()
    rdd2.foreach(println)
    // Using Dataframe
    df1.select("Year","Winner","Runners-Up","Third", "Fourth").filter("Winner == 'Italy'").show(10)
    // usig Spark SQL
    val DFQ2 = spark.sql("select * from WC where Winner = 'Italy' order by Year").show(10)


    // Details of years ending in ZERO
    // RDD
    val rdd3 = data.filter(line => (line.split(",")(7)>"16" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(6), line.split(",")(7))).collect()
    rdd3.foreach(println)

    //DataFrame
    df1.select("Year","Winner","QualifiedTeams").filter("QualifiedTeams > 16").show(10)

    //DF - SQL
    val DFQ3 = spark.sql("SELECT Year, Winner, QualifiedTeams from WC where QualifiedTeams > 16  ").show(10)

    //2014 world cup stats
    //Rdd
    val rdd4 = data.filter(line => line.split(",")(1)==line.split(",")(5))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(5)))
      .collect()
    rdd4.foreach(println)

    // Using Dataframe
    df1.select("Year","Country","Fourth").filter("Country==Fourth").show(10)

    // usig Spark SQL
    val DFQ4 = spark.sql("select Year,Country,Fourth from WC where Country = Fourth order by Year").show()

    //Max matches played
    //RDD
    val rdd5 = data.filter(line=>line.split(",")(8) > "55")
      .map(line=> (line.split(",")(0),line.split(",")(8),line.split(",")(3))).collect()
    rdd5.foreach(println)

    // DataFrame
    df1.filter("MatchesPlayed > 55").show()

    // Spark SQL
    val DFQ5 = spark.sql(" Select * from WC where MatchesPlayed in " +
      "(Select Max(MatchesPlayed) from WC )" ).show()

  }

}