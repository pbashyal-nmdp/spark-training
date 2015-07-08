#####working file for R-connecting to spark cluster
library("dplyr")
library(SparkR)  ###you manually place this file in the R libpaths from the apache 1.4.1 build file
#Sys.setenv('SPARK_HOME'="c:/Users/malbrech/Desktop/spark-1.4.1-bin-hadoop1")
SPARK_HOME <- Sys.getenv("SPARK_HOME")
####this initializes the spark context locally 
sc <- sparkR.init(master = "",
                  appName = "MyApplication",
                  sparkHome = SPARK_HOME,
                  sparkPackages = "com.databricks:spark-csv_2.11:1.0.3")
sqlContext <- sparkRSQL.init(sc)

###make a data frame
df <- createDataFrame(sqlContext, faithful)
explain(df)

local_var_grab <- collect(df) ###grab a DF from the cluster (make it small)

head(df)

###
people <- read.df(sqlContext, paste0(SPARK_HOME,"/examples/src/main/resources/people.json"), "json")
head(people)
printSchema(people)
write.df(people, path="people.parquet", source="parquet", mode="overwrite")

#####dataFrame Operations
###df is hanging around already from before
head(select(df, df$eruptions))
head(select(df, "eruptions"))
head(filter(df, df$waiting < 50))

###complex operations
head(summarize(groupBy(df, df$waiting), count = n(df$waiting)))
waiting_counts <- summarize(groupBy(df, df$waiting), count = n(df$waiting))
head(arrange(waiting_counts, desc(waiting_counts$count)))


###try some dplyr syntax
waiting_counts <- df %>% 
  SparkR::groupBy(df$waiting) %>% 
  SparkR::summarize(count=SparkR::n(df$waiting))
head(waiting_counts)


####Read in a CSV file using external package
download.file("https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv",
              "cars.csv","internal")
check <- read.csv("cars.csv")###
print(check)###result should look like this
df <- read.df(sqlContext, "cars.csv", source = "com.databricks.spark.csv")
collect(df)
write.df(df, "newcars.csv", "com.databricks.spark.csv", "overwrite")

###close the spark context
SparkR::sparkR.stop()







