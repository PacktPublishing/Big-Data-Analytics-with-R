### --- Chapter 8 --- ###
### --- Spark with R --- ###

### --- Spark with R on HDInsight --- ###

Sys.getenv("SPARK_HOME")

Sys.setenv(SPARK_HOME = "/usr/hdp/2.4.1.1-3/spark")
Sys.getenv("SPARK_HOME")
library(rJava)
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sc <- sparkR.init(master="yarn-client", 
                  appName="SparkRStudio", 
                  sparkJars = c("/usr/hdp/2.4.1.1-3/hadoop/hadoop-nfs.jar,
                                /usr/hdp/2.4.1.1-3/hadoop/hadoop-azure.jar,
                                /usr/hdp/2.4.1.1-3/hadoop/lib/azure-storage-2.2.0.jar"))


#Get the status_data from Hive to Spark:

hiveContext <- sparkRHive.init(sc)
hiveContext
tableNames(hiveContext, "bikes")

status.data <- sql(hiveContext, "FROM bikes.status_data SELECT *")
status.data <- table(hiveContext, "bikes.status_data")

columns(status.data)

head(status.data)

count(status.data)

#We need to delete the first row:
status.data <- dropna(status.data)
head(status.data)
dtypes(status.data)
printSchema(status.data)

#Convert the time to actual timestamp:
status.data$time <- regexp_replace(status.data$time, "\"", "")
status.data$datetime <- cast(status.data$time, "timestamp")
printSchema(status.data)
head(status.data)

#Extract specific hour from the timestamp:
status.data$hour <- hour(status.data$datetime)
head(status.data)

output1 <- describe(status.data, "bikes_available", "hour")
output1
showDF(output1)

#Aggregate the mean of available bikes per hour for each station - order by station and hour:
status.data.grouped <- group_by(status.data, "station_id", "hour")
output2 <- summarize(status.data.grouped, 
                     meanBikesAvail = mean(status.data$bikes_available))

#let's clean the output a bit more to order the values by station_id and hour:
showDF(output2)
showDF(arrange(output2, "station_id", "hour", 
               decreasing = c(FALSE, FALSE)))

#pull the output from a SparkR DataFrame object to the native R data.frame:
df.output2 <- as.data.frame(output2)
str(df.output2)
head(df.output2, n=10)

#Merge the resulting DataFrame with the station information data (explore the station info data first):
station.data <- sql(hiveContext, "FROM bikes.station_data SELECT *")
columns(station.data)
head(station.data)
station.data <- dropna(station.data)

output3 <- merge(output2, station.data, by = "station_id")
printSchema(output3)
output3 <- subset(output3, select = c("station_id_x", "name", 
                                      "hour", "meanBikesAvail",
                                      "dockcount"))
head(output3)

#Calculating the percentage of available docks for each station per hour:
coltypes(output3)
output3$dockcount <- cast(output3$dockcount, "double")
output3.grouped <- group_by(output3, "name", "hour")
output4 <- summarize(output3.grouped, 
                     percDocksAvail = sum((output3$dockcount-output3$meanBikesAvail)/output3$dockcount*100))

output4 <- arrange(output4, "name", "hour", 
                   decreasing = c(FALSE, FALSE))

showDF(output4, numRows = 60)

#Show only the stations with the average percentage of docks available 
#equal to or over 70% for a particular hour:
output4.subset <- filter(output4, "percDocksAvail >= 70")
count(output4.subset)
showDF(output4.subset, numRows = 30)

#Which bikes are most used in total - the highest number of seconds in use for each bike per month.
#Sort the use of bikes in decreasing order, identify 20 most used bikes. 
trip.data <- sql(hiveContext, "FROM bikes.trip_data SELECT *")
columns(trip.data)
trip.data <- dropna(trip.data)
head(trip.data)


bikes.used <- subset(trip.data, 
                     select = c("duration", "start_date",
                                "bike_no"))
printSchema(bikes.used)

bikes.used$datetime <- unix_timestamp(bikes.used$start_date,
                                      format = "MM/dd/yyyy HH:mm")

bikes.used$datetime2 <- from_unixtime(bikes.used$datetime, "yyyy-MM-dd")
bikes.used$month <- month(bikes.used$datetime2)
head(bikes.used)

bikes.grouped <- group_by(bikes.used, "month", "bike_no")
bikes.mostused <- summarize(bikes.grouped, 
                            sumDuration = sum(bikes.used$duration))

bikes.mostused <- arrange(bikes.mostused, "month", 
                          "sumDuration", decreasing = c(FALSE, TRUE))

showDF(bikes.mostused) #but this shows all bikes, we only need top 5 for each month

#Download the data:
df.bikes <- as.data.frame(bikes.mostused)
str(df.bikes)
df.bikes.split <- split(df.bikes, df.bikes$month)
topUsage <- do.call(rbind, 
                    sapply(df.bikes.split, 
                           simplify = FALSE,
                           function(x)x[order(x$sumDuration,
                                              decreasing = TRUE), ][1:5,]))

topUsage

#Calculating overall bike usage across the whole year - print 20 bikes with the greatest usage:
head(bikes.used)
bikes.grouped2 <- group_by(bikes.used, "bike_no")
bikes.mostused2 <- summarize(bikes.grouped2,
                             sumDuration = sum(bikes.used$duration))

bikes.mostused2 <- limit(arrange(bikes.mostused2, "sumDuration",
                                 decreasing = TRUE), 20)

showDF(bikes.mostused2)

sparkR.stop(sc)


### --- The end of Chapter 8 --- ###