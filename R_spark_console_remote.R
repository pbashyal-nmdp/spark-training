#####working file for R-connecting to spark cluster
library("dplyr")
library(SparkR)  ###you manually place this file in the R libpaths from the apache 1.4 build file

####this initializes the spark context locally 
sparkRootDir <- "c:/Users/malbrech/Desktop/spark-1.4.0-bin-hadoop1"
sc <- sparkR.init(sparkHome=sparkRootDir)
sqlContext <- sparkRSQL.init(sc)

####this initializes the spark context remotely (i.e. we connect in to a large cluster) 
sc <- sparkR.init(sparkHome=sparkRootDir)
sqlContext <- sparkRSQL.init(sc)

###make a data frame
df <- createDataFrame(sqlContext, faithful)
explain(df)

local_var_grab <- collect(df) ###grab a DF from the cluster (make it small)

head(df)

###
people <- read.df(sqlContext, paste0(sparkRootDir,"/examples/src/main/resources/people.json"), "json")
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


###close the spark context
SparkR::sparkR.stop()







