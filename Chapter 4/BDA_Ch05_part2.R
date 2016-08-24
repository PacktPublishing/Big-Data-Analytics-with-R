### --- Chapter 5 --- ###

### --- Part 1 - R on a multi-node HDInsight Hadoop cluster--- ###

# Data: Energy Demand Research Project (+ 1 additional variable 
##created during the data preparation activities)

# Size: ~13.5 GB; ~ 414,000,000 rows, 5 variables 
# Format: csv without a header due to HDFS framework requirements


# ------ Prepare the data ------ #
# Register at UK Data Service and download the electricity meter readings file from Discover Catalogue:
# https://discover.ukdataservice.ac.uk/catalogue/?sn=7591

# In R on virtual machine set working directory to where the original data file is.
## Run the following lines to prepare the data and create a data file with no header:
library(data.table)
system.time(elecsam <- fread("edrp_elec.csv", sep=",", header=TRUE)) 
gc()
str(elecsam)

library(lubridate)
datetime <- elecsam$ADVANCEDATETIME
gc()
system.time(datetime <- IDateTime(strptime(datetime, "%d%b%y:%H:%M:%S")))
elecsam$itime <- datetime$itime

elecsam$HOUR <- hour(elecsam$itime)
str(elecsam)

elecsam$itime <- NULL

# Set the working directory to where you want the data to be written.
# Write data to a file with no header:
write.table(elecsam, file = "elec_noheader.csv", sep = ",", 
            row.names = FALSE, col.names = FALSE)

# ------ The End of data preparation script ------ #



# ------ HDInsight + R ------ #
# Before we start, we need to set up RHadoop:

# Setting HADOOP_CMD environment variable:
cmd <- system("which hadoop", intern=TRUE)
cmd
Sys.setenv(HADOOP_CMD=cmd)

# Setting HADOOP_STREAMING environment variable:
stream <- system("find /usr -name hadoop-streaming*jar", intern=TRUE)
stream

Sys.setenv(HADOOP_STREAMING=stream[1])

# Checking whether the Hadoop variables are set correctly:
Sys.getenv("HADOOP_CMD")
Sys.getenv("HADOOP_STREAMING")


# Load RHadoop packages:
library(rmr2)
library(rhdfs)

# Start hdfs connection:
hdfs.init()

# Getting the data into DFS:
## List electricity data in local file system:

getwd()
setwd("/home/swalko/data") #set your working directory
getwd()

file <- dir(getwd(), pattern = "_noheader.csv", full.names = TRUE)
file

## Put the file into DFS:

hdfs.mkdir("elec/data") #creates a directory on HDFS
hdfs.put(file, "elec/data") #copies a file (full path) from local file system to a designated directory on HDFS
hdfs.ls("elec/data") #lists files on HDFS
hdfs.ls("elec/data")$file #retrieves file name from HDFS

## Where is the file:

elec.data <- hdfs.ls("elec/data")$file
elec.data

# Create an input format:

elec.format <- read.csv("input_format.csv", sep = ",", header=TRUE, stringsAsFactors = FALSE)
str(elec.format)
elec.format

colClasses <- as.character(as.vector(elec.format[1, ]))
colClasses

data.format <- make.input.format(format = "csv", sep = ",",
                                 col.names = names(elec.format),
                                 colClasses = colClasses,
                                 stringsAsFactors = FALSE)


# MapReduce with a key-value pair:

elec.map <- function(k, v) {
  timestamp <- v[[2]]
  wkday <- weekdays(as.Date(timestamp, format = "%d%b%y"))
  keyval(wkday, 1)
}

mr <- mapreduce(elec.data, input.format = data.format, map = elec.map)

mr()

str(from.dfs(mr))
head(keys(from.dfs(mr)), n=50)
head(values(from.dfs(mr)), n=50)

# MapReduce with a simple Reducer:

elec.map <- function(k, v) {
  timestamp <- v[[2]]
  wkday <- weekdays(as.Date(timestamp, format = "%d%b%y"))
  keyval(wkday, 1)
}

elec.reduce <- function(k, v) {
  keyval(k, sum(v))
}

rmr.options()
rmr.options(backend = "hadoop",
            backend.parameters = list(hadoop = list(D = "mapreduce.map.memory.mb=1024")))

mr <- mapreduce(elec.data, input.format = data.format, map = elec.map, reduce = elec.reduce)

keys(from.dfs(mr))
values(from.dfs(mr))

# More practical MapReduce applications.
## calculate average electricity consumption per Hour across all data points:

elec.map <- function(k, v) {
  keyval(v[[5]], v[[4]])
}

elec.reduce <- function(k, v) {
  data.frame(hour=k, electricity=mean(v), row.names = k)
}

mr <- mapreduce(elec.data, input.format = data.format, map = elec.map, reduce = elec.reduce)

values(from.dfs(mr))

plot1 <- values(from.dfs(mr))

### Drawing a basic line plot using ggplot2 package:
install.packages("ggplot2")
library(ggplot2)

ggplot(plot1, aes(x=factor(hour), y=electricity, group=24)) + 
  geom_line(colour="blue", linetype="longdash", size=1.5) + 
  geom_point(colour="blue", size=4, shape=21, fill="white") +
  xlab("Hour of measurement") + 
  ylab("Units of kilowatt-hours consumed") + 
  ggtitle("A line graph of kilowatt-hour consumed per Hour") +
  theme_bw()

## Creating output format:
out.form <- make.output.format(format = "csv", sep = ",")

## Running the same MapReduce with an output to an arbitrary directory on HDFS:
mr <- mapreduce(elec.data, output = "/user/swalko/output", 
                input.format = data.format, output.format = out.form,
                map = elec.map, reduce = elec.reduce)

mr
hdfs.ls("output")
hdfs.file <- hdfs.ls("output")$file[2]

# Copying the data from HDFS into local file system:
hdfs.get(hdfs.file, "/home/swalko/data/output.txt")

### --- The end of Part 2 - Chapter 5 --- ###