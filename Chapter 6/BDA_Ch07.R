### --- Chapter 7 --- ###
### --- R with NoSQL databases --- ###

### --- Part 1 - MongoDB on a Amazon EC2 instance --- ###


# Variable names and data dictionary: https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd
# Link to download the data: https://data.gov.uk/dataset/land-registry-monthly-price-paid-data

setwd("~/Desktop/B05396_Ch07_Code")

library(rJava)
library(rmongodb)

# Create a connection to a MongoDB running on localhost:
m <- mongo.create()
m

mongo.is.connected(m)
mongo.get.databases(m)

mongo.get.database.collections(m, "houses")
mongo.command(mongo = m, db = "houses", command = list(listCollections=1))

mongo.count(m, "houses.prices")

# Number of transactions of detached houses ("D") in Surrey in 2015:
mongo.count(m, "houses.prices", query = '{"county":"SURREY", "propType":"D"}')

mongo.distinct(m, "houses.prices", "county")
mongo.get.values(m, "houses.prices", "county")

surrey <- mongo.find.one(m, "houses.prices", '{"county":"SURREY"}')
surrey 

# Converting BSON to a list:
mongo.bson.to.list(surrey)

# Use list in queries:
query1 <- mongo.bson.from.list(list("county"="SURREY"))
query1

surrey <- mongo.find.one(m, "houses.prices", query = query1)
surrey

query2 <- mongo.bson.from.list(list("county"="SURREY", "propType"="D"))
query2

surrey <- mongo.find.one(m, "houses.prices", query = query2)
surrey

# Creating an empty 'buffer':
mbuf1 <- mongo.bson.buffer.create()
mbuf1 

# Appending to the 'buffer':
mongo.bson.buffer.append(mbuf1, "county", "SURREY")
mbuf1

query3 <- mongo.bson.from.buffer(mbuf1)
query3

surrey <- mongo.find.one(m, "houses.prices", query = query3)
surrey

mongo.count(m, "houses.prices", query = query3)
surrey <- mongo.find.all(m, "houses.prices", query = query3)
surrey
length(surrey)

# Use skip and limit methods:
surrey <- mongo.find.all(m, "houses.prices", query = query3, skip = 100, limit=100)
surrey
length(surrey)

# Print only fields of interest and sort the data:
fields1 <- mongo.bson.from.list(list("price"=1, "oldNew"=1, "_id"=0))
surrey <- mongo.find.all(m, "houses.prices", query = query3, 
                         skip = 100, limit=100, 
                         fields = fields1, 
                         sort = '{"price": -1}')
surrey

# Unlist to a data.frame:
df <- data.frame(matrix(unlist(surrey), nrow=100, byrow=T),stringsAsFactors=FALSE)
names(df) <- c("price", "oldNew")
head(df, n=10)

# More complex query - create manually:
mbuf2 <- mongo.bson.buffer.create()
mongo.bson.buffer.start.object(mbuf2, 'price')
mongo.bson.buffer.append(mbuf2, '$lt', 300000)
mongo.bson.buffer.finish.object(mbuf2)
mongo.bson.buffer.start.object(mbuf2, 'propType')
mongo.bson.buffer.append(mbuf2, '$eq', "D")
mongo.bson.buffer.finish.object(mbuf2)
mongo.bson.buffer.start.object(mbuf2, 'county')
mongo.bson.buffer.append(mbuf2, '$eq', "GREATER LONDON")
mongo.bson.buffer.finish.object(mbuf2)

query4 <- mongo.bson.from.buffer(mbuf2)
query4

fields2 <- mongo.bson.from.list(list("price"=1, "propType"=1, "county"=1, "district"=1, "_id"=0))
system.time(mfind <- mongo.find(m, 'houses.prices', 
                                query = query4, 
                                fields = fields2,
                                limit = 1000))
mfind

# Create empty vectors for the data:
Price <- Prop_Type <- County <- District <- NULL

while (mongo.cursor.next(mfind)) {
  value <- mongo.cursor.value(mfind)
  Price <- rbind(Price, mongo.bson.value(value, 'price'))
  Prop_Type <- rbind(Prop_Type, mongo.bson.value(value, 'propType'))
  County <- rbind(County, mongo.bson.value(value, 'county'))
  District <- rbind(District, mongo.bson.value(value, 'district'))
}

housesLondon <- data.frame(Price, Prop_Type, County, District)
summary(housesLondon)

# An aggregation pipeline:
agg1 <- mongo.bson.from.JSON('{"$match":
                             {"county":"SURREY"}}')
agg1

agg2 <- mongo.bson.from.JSON('{"$group":
                             {"_id":"$town", 
                             "avgPrice": {"$avg":"$price"}}}')
agg2

agg3 <- mongo.bson.from.JSON('{"$sort":
                             {"avgPrice": -1}}')
agg3


agg4 <- mongo.bson.from.JSON('{"$limit": 5}')
agg4

listagg <- list(agg1, agg2, agg3, agg4)
listagg


output <- mongo.aggregation(m, 'houses.prices', listagg)
output
mongo.bson.to.list(output)

# Close the connection to MongoDB:
mongo.disconnect(m)

# Reconnect quickly:
mongo.reconnect(m)

mongo.destroy(m)
mongo.reconnect(m)

summary(housesLondon)


# --- RMongo package ---#

library(RMongo)

# Create an RMongo object with connection to a specific database:
m <- mongoDbConnect("houses", port=27017)
m

# Create a query using RMongo - issues with a known OutOfMemoryError Java problem for large outputs:
system.time(subset1 <- dbGetQuery(m, "prices", "{'price':{$lt:500000}}", skip=1000, limit=1000))
#options("java.parameters")
#options(java.parameters = "-Xmx16g")
str(subset1)
summary(subset1$price)

system.time(subset2 <- dbGetQuery(m, "prices", "{'price':{$lt:500000}, 
                                  'county':{$eq:'GREATER LONDON'}}", 
                                  skip=0, limit = 10000))
str(subset2)
head(subset2, n=10)

system.time(subset3 <- dbGetQueryForKeys(m, "prices", 
                                         "{'price':{$lt:500000}, 
                                         'county':{$eq:'GREATER LONDON'}}", 
                                         "{'district':1, 'price':1, 'propType':1}",
                                         skip=0, limit = 50000))
str(subset3)
head(subset3, n=10)

# Aggregate function:
houses.agr <- dbAggregate(m, "prices", 
                          c('{"$match": {"county": "SURREY"}}',
                            '{"$group": {"_id": "$town", 
                              "avgPrice": {"$avg": "$price"}}}',
                            '{"$sort": {"avgPrice": -1}}',
                            '{"$limit": 5}'))
houses.agr

require(RJSONIO)

datalist <- lapply(houses.agr, FUN=fromJSON)
datalist
data.df <- data.frame(matrix(unlist(datalist), nrow=5, byrow=T), 
                      stringsAsFactors=FALSE)
names(data.df) <- c("town", "price")
data.df

# Disconnect from MongoDB:
dbDisconnect(m)


# --- mongolite package ---# 
library(mongolite)
help(package="mongolite")

# Create a connection:
m <- mongo(collection = "prices", db = "houses", url = "mongodb://localhost")
m

# Calculate the number of documents (cases) in the collection:
m$count()

# Collection statistics and server info:
m$info()

# Adding and  removing indices from the collections:
m$index()
m$index(add = "propType")

# First find() query:
subset1 <- m$find('{"price":{"$lt":100000}, 
                  "propType":{"$eq":"D"}}')
str(subset1)

# Add other methods and projections to the query:
subset2 <- m$find('{"price":{"$lt":100000}, "propType":{"$eq":"D"}}', 
                  fields = '{"_id":0, "price":1, "town":1}',
                  sort = '{"price":-1}', skip = 0, limit = 10000)
str(subset2)
head(subset2, n=10)
range(subset2$price)

# A map-reduce query:
houses.xtab <- m$mapreduce(
  map = "function(){emit({county:this.county, propType:this.propType}, 1)}",
  reduce = "function(id, counts){return Array.sum(counts)}"
)
houses.xtab

# Unique values for a categorical variable:
m$distinct("county")
m$distinct("propType")

# An aggregation pipeline: 
houses.agr <- m$aggregate('[{"$group": {"_id":"$county", 
                          "count":{"$sum":1}, 
                          "avgPrice":{"$avg":"$price"} }},
                          {"$sort":{"avgPrice": -1} }]')

head(houses.agr, n=10)


### --- The end of Part 1 --- ###

### --- Part 2 - HBase on Microsoft Azure HDInsight --- ###
cmd <- system("which hadoop", intern=TRUE)
cmd
Sys.setenv(HADOOP_CMD=cmd)

stream <- system("find /usr -name hadoop-streaming*jar", intern=TRUE)
stream
Sys.setenv(HADOOP_STREAMING=stream[1])
Sys.getenv("HADOOP_CMD")
Sys.getenv("HADOOP_STREAMING")

library(rmr2)
library(rhdfs)
hdfs.init()
hdfs.ls("/user/swalko")

pp2015.path <- hdfs.ls("/user/swalko")$file[2]
pp2015.path


library(rhbase)
# help(package="rhbase")


hostLoc = '127.0.0.1'
port = 9090  #Default port for Thrift server
hb.init(hostLoc, port, serialize = "character")
hb.list.tables()

hb.describe.table("bigTable")
hb.regions.table("bigTable")

hb.pull("bigTable",
        column_family = "property",
        start = "\"{23B6165E-FED6-FCF4-E050-A8C0620577FA}\"",
        end = "\"{23B6165E-FED6-FCF4-E050-A8C0620577FA}\"",
        batchsize = 100)

hb.pull("bigTable",
        column_family = "transaction:price",
        start = "\"{23B6165E-FED6-FCF4-E050-A8C0620577FA}\"",
        end = "\"{23B6165E-FED6-FCF4-E050-A8C0620577FA}\"",
        batchsize = 100)

iter <- hb.scan("bigTable", 
                startrow = "\"{23B6165E-FED6-FCF4-E050-A8C0620577FA}\"", 
                end = "\"{23B6165F-0452-FCF4-E050-A8C0620577FA}\"",
                colspec = "transaction:price")
while( length(row <- iter$get(1))>0){
  print(row)
}
iter$close()

hb.get("bigTable",
       list("\"{23B6165E-FED6-FCF4-E050-A8C0620577FA}\"", 
            "\"{23B6165F-0452-FCF4-E050-A8C0620577FA}\""))


hb.delete.table("bigTable")


### --- The end of Part 2 --- ###

### --- The end of Chapter 7 --- ###
