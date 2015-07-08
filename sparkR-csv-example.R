# Requires Spark 1.4.1
library(SparkR)

SPARK_HOME <- Sys.getenv("SPARK_HOME")
SPARK_HOME <- "/Users/pbashyal/Spark/spark-1.4.0-bin-hadoop1"

# download.file(url = "https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv", 
#               destfile = 'cars.csv',
#               method = 'curl')  # use 'internal' on Windows

sc <- sparkR.init(master = "local[*]",
                  appName = "MyApplication",
                  sparkHome = SPARK_HOME,
                  sparkPackages = "com.databricks:spark-csv_2.10:1.0.3")

sqlContext <- sparkRSQL.init(sc)
df <- read.df(sqlContext, "cars.csv", source = "com.databricks.spark.csv")

write.df(df, "newcars.csv", "com.databricks.spark.csv", "overwrite")
