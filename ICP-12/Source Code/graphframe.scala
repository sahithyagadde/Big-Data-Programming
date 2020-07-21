import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._

object graphframe{
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Graph")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graphs")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Import the dataset as a csv file and create data framesdirectly on importthan create graph out of the data frame created.

    val trips_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("201508_trip_data.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("201508_station_data.csv")

    // Printing the Schema
    print("Train Data Schema")
    trips_df.printSchema()
    print("Station Data Schema")
    station_df.printSchema()


    //creating three Temp View
    trips_df.createOrReplaceTempView("Trips")
    station_df.createOrReplaceTempView("Stations")

    //Remove duplicates
    print("Remove Duplicates")
    station_df.select("dockcount").distinct().show()
    val tripsduplicates= trips_df.dropDuplicates(trips_df.columns).show()
    val stationduplicates= station_df.dropDuplicates(station_df.columns).show()




    //Name Columns
    //Create vertices
    val nstation = spark.sql("select * from Stations")

    val ntrips = spark.sql("select * from Trips")
    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")
    val stationGraph = GraphFrame(stationVertices, tripEdges)

    //Concatenate chunks into list & convert to DataFrame
    print("Concatenation")
    station_df.select(concat(col("lat"),lit(" "),col("long"))).alias("location").show(10)


    tripEdges.cache()
    stationVertices.cache()

    //Output DataFrame

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + ntrips.count)
    //Show some vertices
    //Show some edges
    print("Vertices")
    stationGraph.vertices.show()

    print("Edges")
    stationGraph.edges.show()

    //Vertex in-Degree

    val inDeg = stationGraph.inDegrees
    println("InDegree" + inDeg.orderBy(desc("inDegree")).limit(5))
    inDeg.show(5)

   //Vertex out-Degree
    val outDeg = stationGraph.outDegrees
    println("OutDegree" + outDeg.orderBy(desc("outDegree")).limit(5))
    outDeg.show(5)

   //Apply the motif findings
    print("Motif findings")
    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()

    //Apply Stateful Queries.
    print("Stateful Queries")
   val stateful = stationGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
    stateful.show()

    // Subgraphs with a condition.

    print("Subgraph")

    val subgraph1 = stationGraph.find("(a)-[e]->(b)")
                   .filter("e.Duration>932")
                    .show()






   // Vertex degree
    print("Vertex Degree")
    val ver = stationGraph.degrees
    ver.show(5)
    println("Degree" + ver.orderBy(desc("Degree")).limit(5))


    //What are the most common destinations in the dataset from location to location?
    print("most common destinations in the dataset from location to location")
    val heighestdestination = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    heighestdestination.show(10)

    //What is the station with the highest ratio of in degrees but fewest out degrees. As in, what station acts as almost a pure trip sink. A station where trips end at but rarely start from.
    print("station with the highest ratio of in degrees but fewest out degrees")
    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

    degreeRatio.cache()
    degreeRatio.orderBy(desc("degreeRatio")).show(10)
   // Save graphs generated to a file.
    stationGraph.vertices.write.csv("/Users/sahithyagadde/Downloads/Graphs/vertices.csv")

    stationGraph.edges.write.csv("/Users/sahithyagadde/Downloads/Graphs/edges.csv")





  }
}