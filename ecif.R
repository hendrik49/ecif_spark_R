library(sparklyr)
library(dplyr)
library(odbc)
library(DBI)

conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "16G"  
conf$spark.memory.fraction <- 0.8 
config <- spark_config()
config$`sparklyr.shell.driver-class-path` <- 
  "lib/postgresql-9.3-1104.jdbc4.jar"

sc <- spark_connect(master = "local", app_name = "ECIF SPark Engine",  config = config)

individualgoldendata <- spark_read_jdbc(sc, "ecif_jdbc",  options = list(
  url = "jdbc:postgresql://localhost:5432/ECIF",
  user = "postgres",
  password = "postgres",
  dbtable = "public.individualgoldendata"))

individualgoldendatahistory <- spark_read_jdbc(sc, "ecif_jdbc",  options = list(
  url = "jdbc:postgresql://localhost:5432/ECIF",
  user = "postgres",
  password = "postgres",
  dbtable = "public.individualgoldendatahistory"))

individualdatasteward <- spark_read_jdbc(sc, "ecif_jdbc",  options = list(
  url = "jdbc:postgresql://localhost:5432/ECIF",
  user = "postgres",
  password = "postgres",
  dbtable = "public.individualdatasteward"))

