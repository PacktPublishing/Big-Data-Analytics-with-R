### --- Chapter 6 --- ###
### --- R with Relational Database Management Systems (RDBMSs) --- ###

### --- Part 1 - SQLite on local machine --- ###

setwd("~/Desktop/B05396_Ch06_Code")

# install.packages("devtools")
devtools::install_github("RcppCore/Rcpp")
devtools::install_github("rstats-db/DBI") #make sure to install the new version of DBI; you may have to detach
# all packages that use DBI library and the previously installed DBI library as well; then re-install the DBI package
# before installing RSQLite

install.packages("RSQLite")
library(RSQLite)

con <- dbConnect(RSQLite::SQLite(), "need_data")
con

dbListTables(con)
dbListFields(con, "need")

query.1 <- dbSendQuery(con, "SELECT * FROM need WHERE FLOOR_AREA_BAND = 1")
dbGetStatement(query.1)

info <- dbGetInfo(query.1)
info

query.1.res <- fetch(query.1, n=50)
str(query.1.res)
query.1.res

dbColumnInfo(query.1)
info <- dbGetInfo(query.1)
info
str(info)
info$fields

dbClearResult(query.1)

query.2 <- dbSendQuery(con, "SELECT EE_BAND, PROP_AGE, PROP_TYPE, 
                       AVG(Econs2012) AS 'AVERAGE_ELEC_2012' 
                       FROM need 
                       GROUP BY EE_BAND, PROP_AGE, PROP_TYPE 
                       ORDER BY EE_BAND, PROP_TYPE ASC")
query.2.res <- fetch(query.2, n=-1)
info2 <- dbGetInfo(query.2)
info2
head(query.2.res, n=6)

dbWriteTable(con, "query_2_result", query.2.res)
dbListTables(con)

dbClearResult(query.2)
dbDisconnect(con)

### --- The end of Part 1 --- ###

### --- Part 2 - MariaDB on Amazon EC2 --- ###
install.packages("RMySQL")
library(RMySQL)

conn <- dbConnect(RMySQL::MySQL(), user = "swalko",
                  password = "Password1",
                  host = "localhost",
                  dbname = "data1")

conn <- dbConnect(RMySQL::MySQL(), group = "dt1", 
                  host = "localhost")

summary(conn)

dbGetInfo(conn)

dbListTables(conn)
dbListFields(conn, "need")

query.1 <- dbSendQuery(conn, "SELECT COUNT(*) AS records FROM need")
query.1
dbGetStatement(query.1)
dbColumnInfo(query.1)
dbGetInfo(query.1)

query.1.res <- dbFetch(query.1, n=-1)
query.1.res

dbClearResult(query.1)

query.2 <- dbSendQuery(conn, "SELECT EE_BAND, PROP_AGE, PROP_TYPE, 
                       AVG(Econs2012) AS AVERAGE_ELEC_2012 
                       FROM need 
                       GROUP BY EE_BAND, PROP_AGE, PROP_TYPE 
                       ORDER BY EE_BAND, PROP_TYPE ASC")

query.2
dbGetStatement(query.2)
dbColumnInfo(query.2)
dbGetInfo(query.2)

query.2.res <- dbFetch(query.2, n=-1)
query.2.res

dbClearResult(query.2)

dbDisconnect(conn)

gc()

## Querying MariaDB with dplyr package

library(dplyr)
dpl.conn <- src_mysql(dbname = 'data1', 
                      host = 'localhost',
                      user = NULL,
                      password = NULL,
                      group = 'dt1')

dpl.conn

need.data <- tbl(dpl.conn, "need")
need.data

glimpse(need.data)
str(need.data)

## 1. Calculating average electricity consumption by geographical region
## (region) and property type (prop_type) from 2005 to 2012.
## Order by region and property type.

by.regiontype <- group_by(need.data, region, prop_type)
by.regiontype

avg.elec <- summarise(by.regiontype,
                      elec2005 = mean(econs2005),
                      elec2006 = mean(econs2006),
                      elec2007 = mean(econs2007),
                      elec2008 = mean(econs2008),
                      elec2009 = mean(econs2009),
                      elec2010 = mean(econs2010),
                      elec2011 = mean(econs2011),
                      elec2012 = mean(econs2012))
avg.elec <- arrange(avg.elec, region, prop_type)
avg.elec

show_query(avg.elec)
explain(avg.elec)

#As grouped data.frame:
elec.df <- collect(avg.elec)
elec.df

table(elec.df$region)
elec <- as.data.frame(elec.df) #note that as_data_frame() from dplyr will convert the object into tbl, which is not what we need
elec

elec.l <- reshape(elec, 
                  varying = c("elec2005", "elec2006", "elec2007", "elec2008", 
                              "elec2009", "elec2010", "elec2011", "elec2012"),
                  v.names = "electricity",
                  timevar = "year",
                  times = c("2005", "2006", "2007", "2008",
                            "2009", "2010", "2011", "2012"), 
                  direction = "long")

head(elec.l, n=6)


# Re-labelling the values of region and property type:

elec.l <- within(elec.l, {
  region[region=="E12000001"] <- "North East"
  region[region=="E12000002"] <- "North West"
  region[region=="E12000003"] <- "Yorkshire and The Humber"
  region[region=="E12000004"] <- "East Midlands"
  region[region=="E12000005"] <- "West Midlands"
  region[region=="E12000006"] <- "East of England"
  region[region=="E12000007"] <- "London"
  region[region=="E12000008"] <- "South East"
  region[region=="E12000009"] <- "South West"
  region[region=="W99999999"] <- "Wales"
})

elec.l <- within(elec.l, {
  prop_type[prop_type==101] <- "Detached house"
  prop_type[prop_type==102] <- "Semi-detached house"
  prop_type[prop_type==103] <- "End terrace house"
  prop_type[prop_type==104] <- "Mid terrace house"
  prop_type[prop_type==105] <- "Bungalow"
  prop_type[prop_type==106] <- "Flat (incl. maisonette)"
})

head(elec.l, n=6)

library(ggplot2)
ggplot(elec.l, aes(x=year, y=electricity, group=factor(prop_type), colour=factor(prop_type))) +
  geom_line() + geom_point() +
  facet_wrap(~region, nrow = 2) +
  scale_colour_discrete(name="Property Type") +
  theme(axis.text.x = element_text(angle = 90),
        panel.grid.major=element_line(colour = "white"),
        panel.grid.minor=element_blank(),
        panel.background=element_rect(fill = "#f6f7fb"), 
        strip.background = element_rect(colour = "#f6f7fb", fill = "#d6e8ff"))


### --- The end of Part 2 --- ###

### --- Part 3 - PostgreSQL through Amazon RDS --- ###

# Install RPostgres from GitHub:
# install.packages("devtools")
devtools::install_github("RcppCore/Rcpp")
devtools::install_github("rstats-db/DBI") 
devtools::install_github("rstats-db/RPostgres")

library(DBI)
library(RPostgres)

con <- dbConnect(RPostgres::Postgres(), dbname = 'data1', 
                 host = 'database1.cgsn1orvgmc4.eu-west-1.rds.amazonaws.com',
                 port = 5432,
                 user = 'swalko',
                 password = 'Password1')

con

dbListTables(con)

dbListFields(con, "mot")

query.1 <- dbSendQuery(con, "SELECT make, testresult,
                       COUNT(*) AS count
                       FROM mot
                       GROUP BY make, testresult
                       ORDER BY make, testresult ASC")

query.1

query.1.res <- dbFetch(query.1, n = -1)
query.1.res

dbClearResult(query.1)

## Get the frequencies for Failed (F) only and add the newly created average mileage (avg_miles) column to the output:
query.2 <- dbSendQuery(con, "SELECT make, testresult,
                       COUNT(*) AS count,
                       AVG(testmileage) AS avg_miles
                       FROM mot
                       WHERE testresult = 'F'
                       GROUP BY make, testresult
                       ORDER BY avg_miles DESC")

query.2

query.2.res <- dbFetch(query.2, n = -1)
query.2.res

dbClearResult(query.2)

dbDisconnect(con)

## Using dplyr package with PostgreSQL database
install.packages("RPostgreSQL")
library(RPostgreSQL)

install.packages("dplyr")
library(dplyr)

dpl.conn <- src_postgres(dbname = 'data1', 
                         host = 'database1.cgsn1orvgmc4.eu-west-1.rds.amazonaws.com',
                         port = 5432,
                         user = 'swalko',
                         password = 'Password1')

dpl.conn

mot.data <- tbl(dpl.conn, "mot")
mot.data

glimpse(mot.data)

str(mot.data)

### 1. Calculating the average mileage for failed vehicles of each make - only for makes with at least 50 failed mots.
### Order by mileage in descending order. 
mot.failed <- filter(mot.data, testresult == "F")
mot.failed
by.make <- group_by(mot.failed, make)
avg.mileage <- summarise(by.make, 
                         count = n(),
                         avg = mean(testmileage))
avg.mileage <- arrange(filter(avg.mileage, count >= 50), desc(avg))
avg.mileage

show_query(avg.mileage)
explain(avg.mileage)

mileage.df <- collect(avg.mileage)
mileage.df

### 2. Calculating the average age and mileage of vehicles that either passed and failed the mot for each make. 
### Show only makes with at least 50 vehicles.
### Order makes alphabetically.
dpl.conn <- src_postgres(dbname = 'data1', 
                         host = 'database1.cgsn1orvgmc4.eu-west-1.rds.amazonaws.com',
                         port = 5432,
                         user = 'swalko',
                         password = 'Password1')

mot.data <- tbl(dpl.conn, "mot")

mot.pf <- filter(mot.data, testresult == "F" | testresult == "P")
mot.pf 

age <- tbl(dpl.conn, sql("SELECT testid, vehicleid, testdate::date - firstusedate::date as age from mot"))
age
mot.combined <- inner_join(mot.pf, age, by = c("testid", "vehicleid"))

by.maketest <- group_by(mot.combined, make, testresult)
avg.agemiles <- summarise(by.maketest, 
                          count = n(),
                          age = mean(age/365.25), 
                          mileage = mean(testmileage))
avg.agemiles <- arrange(filter(avg.agemiles, count >= 50), desc(make))
avg.agemiles

explain(avg.agemiles)

agemiles.df <- collect(avg.agemiles)
agemiles.df

### --- The end of Part 3 --- ###

### --- The end of Chapter 6 --- ###