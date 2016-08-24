### --- Chapter 5 --- ###

### --- Part 1 - Rhadoop tutorial with Mark Twain works on Project Gutenberg --- ###

# For data go to https://www.gutenberg.org/ebooks/3200
# Save the data file as Plain Text UTF-8 to a file named: twain_data.txt

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
library(plyrmr)

# Start hdfs connection:
hdfs.init()


# Getting the data into DFS:
## List twain_data.txt in local file system:

getwd()
setwd("...") #set your working directory
getwd()

file <- dir(getwd(), pattern = "_data.txt", full.names = TRUE)
file

## Put the file into DFS:
hdfs.mkdir("twain") #creates a directory on HDFS
hdfs.ls("twain")

hdfs.put(file, "twain") #copies a file (full path) from local file system to a designated directory on HDFS
hdfs.ls("twain") #lists files on HDFS
hdfs.ls("twain")$file #retrieves file name from HDFS
twain.path <- hdfs.ls("twain")$file

# Create an input format:
twain.format <- make.input.format(format = "text", mode = "text")


# A simple mapper with Twain data - mapping particular words. 
twain.map <- function(k, v) {
  words <- unlist(strsplit(v, " "))
  keyval(words, 1)
}

mr <- mapreduce(twain.path, input.format = twain.format, map = twain.map)

mr()

str(from.dfs(mr))
head(unique(keys(from.dfs(mr))), n=50)
head(values(from.dfs(mr)), n=50)

# A wordcount job - mapper + reducer
twain.map <- function(k, v) {
  words <- unlist(strsplit(v, " "))
  words <- gsub("[[:punct:]]", "", words, perl = TRUE)
  words <- tolower(words)
  keyval(words, 1)
}

twain.reduce <- function(k, v) {
  keyval(k, sum(v))
}

mr <- mapreduce(twain.path, input.format = twain.format, map = twain.map, reduce = twain.reduce)

length(unique(keys(from.dfs(mr))))

head(unique(keys(from.dfs(mr))), n=50)
head(values(from.dfs(mr)), n=50)

# Extracting data from dfs to R object:
output <- from.dfs(mr, format = "native")
str(output)

# Extracting data from dfs to csv file:
out.form <- make.output.format(format = "csv", sep = ",")

mr <- mapreduce(twain.path, output = "/user/swalko/out1", 
                input.format = twain.format, output.format = out.form,
                map = twain.map, reduce = twain.reduce)

mr
wd <- getwd()
wd
hdfs.get(mr, wd)

hdfs.get("/user/swalko/out1/part-00000", 
         "/home/swalko/data/output.txt")

### --- The end of Part 1 - Chapter 5 --- ###