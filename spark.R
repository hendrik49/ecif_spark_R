#install.packages("sparklyr")
#install.packages("dplyr")

library(sparklyr)
library(dplyr)
library(DBI)
#spark_install(version = "2.1.0")

conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "16G"  
conf$spark.memory.fraction <- 0.8 

sc <- spark_connect(master = "local")

if(!file.exists("data"))dir.create("data")

if(!file.exists("data/2003.csv.bz2")){
  download.file("http://stat-computing.org/dataexpo/2009/2003.csv.bz2", "data/2003.csv.bz2")
}

if(!file.exists("data/2004.csv.bz2")){
  download.file("http://stat-computing.org/dataexpo/2009/2004.csv.bz2", "data/2004.csv.bz2")
}

top_rows <- read.csv("data/2003.csv.bz2", nrows = 5)
file_columns <- top_rows %>% 
  purrr::map(function(x)"character")
rm(top_rows)

sp_flights <- spark_read_csv(sc, 
                             name = "flights", 
                             path = "data", 
                             memory = FALSE, 
                             columns = file_columns, 
                             infer_schema = FALSE)

object.size(sp_flights)


#sql
top10 <- dbGetQuery(sc, "Select * from flights limit 10")

top10


#dyplr Use dplyr verbs to interact with the data
flights_table <- sp_flights %>%
  mutate(DepDelay = as.numeric(DepDelay),
         ArrDelay = as.numeric(ArrDelay),
         SchedDeparture = as.numeric(CRSDepTime)) %>%
  select(Origin, Dest, SchedDeparture, ArrDelay, DepDelay, Month, DayofMonth)

flights_table %>% head

#Use show_query() to display what is the SQL query that dplyr will send to Spark
sp_flights  %>% 
  head %>% 
  show_query()

#Cache Data in Spark
sp_flights %>%
  tally

#compute() caches a Spark DataFrame into memory
subset_table <- flights_table %>% 
  compute("flights_subset")

subset_table %>%
  tally


subset_table %>% 
  group_by(Origin) %>%
  tally