## Spark – Lightning Fast Analytics Across a cluster

#### 001_preflight_check

Start up a spark shell in scala

```sh
./bin/spark-shell
```
If Spark gives Networking error, use the following to force spark to use the local network.
```sh
SPARK_LOCAL_IP=127.0.0.1 ./bin/spark-shell
```

To reduce the verbose log, copy ``$SPARK_HOME/conf/log4j.properties.template`` to ``$SPARK_HOME/conf/log4j.properties`` and then change INFO to WARN for root logger.

```diff
-log4j.rootCategory=INFO, console
+log4j.rootCategory=WARN, console
```

```scala
sc
sc.master
```
```scala
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```

#### 002_spark_deconstructed

```scala
// ETL
val lines = sc.textFile("README.md")

// Transformations
val errors = lines.filter(_.startsWith("ERROR"))
val messages = errors.map(_.split("\t")).map(r => r(1))
messages.cache() // Keep this RDD in memory if you can

// Actions
messages.filter(_.contains("mysql")).count()
messages.filter(_.contains("php")).count()
```
The driver is where your spark shell or your application&nbsp;is running. The driver knows how to talk to the cluster.
You can look at the lineage graph or operator graph by

```scala
messages.toDebugString
```

#### 003_a_brief_history

Latency increases as number of different systems need to work together and data needs to be moved around(wire tax). You want a common framework that can run these kinds of task, not just map/reduce.
Uses native runtime systems for running each language. Uses native Python and Py4J gateway.

#### 004_simple_spark_apps

*wordcount.scala*
```scala
val f = sc.textFile("README.md")
// Transform 
// For each line split it into keywords, and map each keyword to a tuple with word with count 1
// Flatmap flattens the result so that you only have a list of keywords, rather than lists of lists
// reduceByKey, gets all the data in each node to a 
val wc = f.flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
// isaveAsTextFile cause the lineage graph to be evaluated, take the results in parallel and save the results as partitioned file
wc.saveAsTextFile("wc_out") // will output  part-00000 and  part-00001 in wc_out directory
```
**wordcount.py**
```scala
from operator import add
f = sc.textFile('README')
wc = f.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
wc.saveAsTextFile("wc_out")
```

**Joins**
```scala
val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

// Provides schema for reg.tsv file
case class Register(d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float)

// Provides schema for click.tsv file
case class Click(d: java.util.Date, uuid: String, landing_page: Int)

// Read reg.tsv, split on tab character and create a (key, value) tuple with uuid as key
// and Register class as value
val reg = sc.textFile("reg.tsv").
          map(_.split("\t")).
          map(r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat)))

// Read clk.tsv, split on the tab character and create a (key, value) with uuid as key
// and Click class as a value
val clk = sc.textFile("clk.tsv").
          map(_.split("\t")).
          map(r => (r(1), Click(format.parse(r(0)), r(1), r(2).toInt)))
          
// Join by the common element: uuid
reg.join(clk).collect()
```

**Exercise**
```scala
// Scala
// Read lines from text files
val readme = sc.textFile("README.md")
val changes = sc.textFile("CHANGES.txt")
// Filter 'Spark' lines and perform word counts on those lines
val readmeSparkWords = readme.filter(_.contains("Spark")).flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
val changesSparkWords = changes.filter(_.contains("Spark")).flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

// Perform join
 readmeSparkWords.join(changesSparkWords).map(kv => (kv._1, kv._2._1 + kv._2._2)).collect()
```
#### 005_spark_essentials
RDD - Distributed Data Set 
Creating RDDs from native list
```scala
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
```
```python
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
```

Using Hadoop Connectors
```scala
val distData = sc.textFile("README.md")
```
```scala
distData = sc.textFile("README.md")
```

Trasformations
```scala
// Scala
val distFile = sc.textFile("README.md")
// Difference between map and flatMap
distFile.map(l => l.split(" ")).collect()
distFile.flatMap(l => l.split(" ")).collect()
```
```python
# Python
distFile = sc.textFile("README.md")
# Difference between map and flatMap
distFile.map(lambda l: l.split(" ")).collect()
distFile.flatMap(lambda l: l.split(" ")).collect()
```
#### 006_spark_examples
Get the security file for Mark Albrecht's twitter account from email and put it in the spark main directory.  The text will look like this
```
debug=true
oauth.consumerKey=XXX
oauth.consumerSecret=XXX
oauth.accessToken=XXX
oauth.accessTokenSecret=XXX
```
this is the twitter API code to be entered into the bash terminal after your twitter4j properties file is created and saved on the main spark folder directory (i.e. the home directory from which this command is run)

```sh
./bin/spark-submit \
--class "org.apache.spark.examples.streaming.TwitterAlgebirdHLL" \
--master "local[*]" \
lib/spark-examples-*.jar
```
For the CMS program streaming example

```sh
./bin/spark-submit \
--class "org.apache.spark.examples.streaming.TwitterAlgebirdCMS" \
--master "local[*]" \
lib/spark-examples-*.jar
```

to cross check an id on Twitter go to http://tweeterid.com/


#### 007_unifying_the_pieces_spark_sql

```scala
// sqlContext is initialized as part of the spark-shell (you have to run the line below)
//val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
case class Person(name: String, age: Int)
val people = sc.textFile("/usr/local/spark/examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
// SchemaRDD migrated to DataFrame.   
people.toDF().registerTempTable("people")  
val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
// Show the query plan
teenagers.explain
// Print the results
teenagers.collect()
// Save as parquet file
people.toDF().saveAsParquetFile("people.parquet")

// Read a parquet file
val parquetFile = sqlContext.read.parquet("people.parquet")
parquetFile.schema
parquetFile.registerTempTable("parquetFile")
val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenagers.explain

// DSL
```
Using Spark SQL DSL
```scala
// sqlContext is initialized as part of the spark-shell
// val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
case class Person(name: String, age: Int)
val people = sc.textFile("/usr/local/spark/examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
val teenagers = people.where('age >= 13).where('age <= 19).select('name)
teenagers.explain
teenagers.collect()
```
IPython Notebook
```sh
#this will not work on  pradeep's box
IPYTHON_OPTS="notebook" /usr/local/spark/bin/pyspark
```
```python
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc) 

lines = sc.textFile("/usr/local/spark/examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name = p[0], age = int(p[1])))

people.collect()

peopleTable = sqlContext.createDataFrame(people)
peopleTable.registerTempTable("people")

teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

teennames = teenagers.map(lambda p: "Name:" + p.name)
teennames.collect()
```
#### 008_unifying_the_pieces_spark_streaming
```sh
nc -lk 9999
```
```sh
#Scala
 /usr/local/spark/bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999
```
```sh
# Python
/usr/local/spark/bin/spark-submit /usr/local/spark/examples/src/main/python/streaming/network_wordcount.py localhost 9999
```

Here is the stateful example that updates a data stream using the same setup
```sh
# Python
/usr/local/spark/bin/spark-submit /usr/local/spark/examples/src/main/python/streaming/stateful_network_wordcount.py localhost 9999
```
#### 009_MLLIB_and_GraphX
GraphX example

```sh
#launch the spark-shell
/usr/local/spark/bin/spark-shell
```

```scala
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

case class Peep(name: String, age: Int)
val nodeArray = Array(
(1L, Peep("Kim", 23)), (2L, Peep("Pat", 31)),
(3L, Peep("Chris", 52)), (4L, Peep("Kelly", 39)),
(5L, Peep("Leslie", 45))
)
val edgeArray = Array(
Edge(2L, 1L, 7), Edge(2L, 4L, 2),
Edge(3L, 2L, 4), Edge(3L, 5L, 3),
Edge(4L, 1L, 1), Edge(5L, 3L, 9)
)

val nodeRDD: RDD[(Long, Peep)] = sc.parallelize(nodeArray)
val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
val g: Graph[Peep, Int] = Graph(nodeRDD, edgeRDD)

val results = g.triplets.filter(t => t.attr > 7)

for (triplet <- results.collect) {
println(s"${triplet.srcAttr.name} loves ${triplet.dstAttr.name}")
}
```

Pregel example for quickest path in graph

```scala
import org.apache.spark.graphx._
// Import random graph generation library
import org.apache.spark.graphx.util.GraphGenerators
// A graph with edge attributes containing distances
val graph  = GraphGenerators.logNormalGraph(sc, numVertices = 5,numEParts=sc.defaultParallelism,mu=4.0,sigma=1.3).mapEdges(e => e.attr.toDouble)

graph.edges.foreach(println)
  
val sourceId: VertexId = 0 // The ultimate source

// Initialize the graph such that all vertices except the root have distance infinity.

val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
val sssp = initialGraph.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => math.min(dist, newDist), 
  // Vertex Program
  triplet => {  // Send Message
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a,b) => math.min(a,b) // Merge Message
  )
println(sssp.vertices.collect.mkString("\n"))

```
#### 010_Unified_Work_Flows_Example
twitter classifier example

```sh
#download the data bricks code from github
git clone https://github.com/databricks/reference-apps.git
#install the simple build tool for scala
cd reference-apps/twitter_classifier/scala
#compile the scala code
sbt/sbt assembly
```

step 1 collect a data base of tweets
```sh
    ${YOUR_SPARK_HOME}/bin/spark-submit \
     --class "com.databricks.apps.twitter_classifier.Collect" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     ${YOUR_OUTPUT_DIR:-/tmp/tweets} \
     ${NUM_TWEETS_TO_COLLECT:-10000} \
     ${OUTPUT_FILE_INTERVAL_IN_SECS:-10} \
     ${OUTPUT_FILE_PARTITIONS_EACH_INTERVAL:-1} \
     --consumerKey ${YOUR_TWITTER_CONSUMER_KEY} \
     --consumerSecret ${YOUR_TWITTER_CONSUMER_SECRET} \
     --accessToken ${YOUR_TWITTER_ACCESS_TOKEN}  \
     --accessTokenSecret ${YOUR_TWITTER_ACCESS_SECRET}
```

run the examine tweets and train k-means classifier (needs to be run on 1.2.2 spark for RDD functions have changed to DF functions)
```sh
     ${YOUR_SPARK_HOME}/bin/spark-submit \
     --class "com.databricks.apps.twitter_classifier.ExamineAndTrain" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     "${YOUR_TWEET_INPUT:-/tmp/tweets/tweets*/part-*}" \
     ${OUTPUT_MODEL_DIR:-/tmp/tweets/model} \
     ${NUM_CLUSTERS:-10} \
     ${NUM_ITERATIONS:-20}

```
Predict a new stream of tweets coming in to a trained cluster (i.e. only print those that match)
```sh
     ${YOUR_SPARK_HOME}/bin/spark-submit \
     --class "com.databricks.apps.twitter_classifier.Predict" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     ${YOUR_MODEL_DIR:-/tmp/tweets/model} \
     ${CLUSTER_TO_FILTER:-1} \
     --consumerKey ${YOUR_TWITTER_CONSUMER_KEY} \
     --consumerSecret ${YOUR_TWITTER_CONSUMER_SECRET} \
     --accessToken ${YOUR_TWITTER_ACCESS_TOKEN}  \
     --accessTokenSecret ${YOUR_TWITTER_ACCESS_SECRET}
```

To check the running jobs with the spark admin tool go to:  http://d0503433.nmdp.org:4040/jobs/



