### --- Chapter 3 --- ###

### --- Part 1 - Out-of-memory data with ff, ffbase and bigmemory --- ###

# --- ff and ffbase packages --- #

#All flights to and from all American airports in September and October 2015.
#951,111 cases over 28 variables; comma-separated text file
#Metadata: airlines IDs:  
#Downloaded from Bureau of Transportation Statistics:
#http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time

#Set working directory to the data and airline_id files.
setwd("...")
getwd()

install.packages("ff")
library(ff)

install.packages("ffbase")
library(ffbase)

# 1. Upload flights_sep_oct15.txt and airline_id.csv files from flat files. 

getwd()
system("mkdir ffdf")
options(fftempdir = ".../ffdf")

system.time(flights.ff <- read.table.ffdf(file="flights_sep_oct15.txt", 
                                     sep=",", VERBOSE=TRUE, 
                                     header=TRUE, next.rows=100000, colClasses=NA))

system.time(airlines.ff <- read.csv.ffdf(file="airline_id.csv", 
                                         VERBOSE=TRUE, header=TRUE, 
                                         next.rows=100000, colClasses=NA))


##Using read.table()
system.time(flights.table <- read.table("flights_sep_oct15.txt", 
                                        sep=",", header=TRUE))

gc()

system.time(airlines.table <- read.csv("airline_id.csv", header = TRUE))


# 2. Inspect the ffdf objects.
## For flights.ff object:
class(flights.ff)
dim(flights.ff)
dimnames.ffdf(flights.ff)

str(flights.ff)
str(flights.ff[1:20,])
head(flights.ff)
head(flights.ff[["ORIGIN_STATE_NM"]], n=30)

## For airlines.ff object:
class(airlines.ff)
dim(airlines.ff)
dimnames.ffdf(airlines.ff)

str(airlines.ff)
str(airlines.ff[1:20,])
head(airlines.ff)
head(airlines.ff[[2]], n=30)

# 3. Merge both files by AIRLINE_ID variable.
## Rename "Code" variable from airlines.ff to "AIRLINE_ID" and "Description" into "AIRLINE_NM".
names(airlines.ff) <- c("AIRLINE_ID", "AIRLINE_NM")
names(airlines.ff)
str(airlines.ff[1:20,])

flights.data.ff <- merge.ffdf(flights.ff, airlines.ff, by="AIRLINE_ID")
#The new object is only 551.2 Kb in size
class(flights.data.ff)
dim(flights.data.ff)
dimnames.ffdf(flights.data.ff)
head(flights.data.ff[[29]], n=30)


##For flights.table:
names(airlines.table) <- c("AIRLINE_ID", "AIRLINE_NM")
names(airlines.table)
str(airlines.table[1:20,])

flights.data.table <- merge(flights.table, airlines.table, by="AIRLINE_ID")
#The new object is already 105.7 Mb in size
#A rapid spike in RAM use when processing

# 4. Check how many categories in ORIGIN_STATE_NM.
system.time(origin_st <- unique(flights.data.ff$ORIGIN_STATE_NM))
origin_st

# 5. Run the frequency test on ORIGIN_STATE_NM variable.
system.time(orig_state_tab <- table.ff(flights.data.ff$ORIGIN_STATE_NM, 
                                       exclude = NA))
orig_state_tab

# 6. Some basic descriptives on DISTANCE and DEP_DELAY.
mean(flights.data.ff$DISTANCE)
quantile(flights.data.ff$DISTANCE)
range(flights.data.ff$DISTANCE)

library(Hmisc)
Hmisc::describe(as.data.frame.ffdf(flights.data.ff$DISTANCE)) 
summary(as.data.frame.ffdf(flights.data.ff$DISTANCE))

Hmisc::describe(as.data.frame.ffdf(flights.data.ff$DEP_DELAY)) 
summary(as.data.frame.ffdf(flights.data.ff$DEP_DELAY))

# 7. Convert numeric ff DAY_OF_WEEK vector to a ff factor:
table.ff(flights.data.ff$DAY_OF_WEEK)

flights.data.ff$WEEKDAY <- cut.ff(flights.data.ff$DAY_OF_WEEK, 
                                   breaks = 7, 
                                   labels = c("Monday", "Tuesday", 
                                              "Wednesday", "Thursday", 
                                              "Friday", "Saturday",
                                              "Sunday"))


# 8. Cross-tabulate DEP_DELAY by specific ORIGIN_CITY_NAME:
library(doBy)

DepDelayByOrigCity <- ffdfdply(flights.data.ff, 
                               split = flights.data.ff$ORIGIN_CITY_NAME,
                               FUN=function(x) {
                                 summaryBy(DEP_DELAY~ORIGIN_CITY_NAME, 
                                           data=x, FUN=mean, na.rm=TRUE)}
                                       )
DepDelayByOrigCity


# 9. Prepare the aggregated ffdf object for visualization.
plot1.df <- as.data.frame.ffdf(DepDelayByOrigCity)
str(plot1.df)

plot1.df <- orderBy(~-DEP_DELAY.mean, data=plot1.df)
plot1.df


# 10. Subset the ffdf object flights.data.ff:

subs1.ff <- subset.ffdf(flights.data.ff, CANCELLED == 1, 
                        select = c(FL_DATE, AIRLINE_ID, 
                                   ORIGIN_CITY_NAME,
                                   ORIGIN_STATE_NM,
                                   DEST_CITY_NAME,
                                   DEST_STATE_NM,
                                   CANCELLATION_CODE))

str(subs1.ff)
dim(subs1.ff)

system.time(subs1.table <- subset(flights.data.table, 
                                  CANCELLED == 1, 
                                  select = c(FL_DATE, AIRLINE_ID, 
                                             ORIGIN_CITY_NAME,
                                             ORIGIN_STATE_NM,
                                             DEST_CITY_NAME,
                                             DEST_STATE_NM,
                                             CANCELLATION_CODE)))


# 11. Save a newly created ffdf object to a data file:

save.ffdf(subs1.ff) #7 files (one for each column) created in the ffdb directory

# 12. Loading previously saved ffdf files:
rm(subs1.ff)
gc()
getwd()
load.ffdf("/.../ffdb")
str(subs1.ff)
dim(subs1.ff)
dimnames(subs1.ff)

# 13. Export subs1.ff into CSV and TXT files:
write.csv.ffdf(subs1.ff, "subset1.csv", VERBOSE = TRUE)

# 14. ffbase2 package:
install.packages("devtools")
devtools::install_github("edwindj/ffbase2")
library(ffbase2)

# 15. bigglm.ffdf() on large data:
install.packages("biglm")
library(biglm)

# Chronic Kidney Disease Dataset available from the Machine Learning Depository
# maintained by the University of California Irvine at http://archive.ics.uci.edu/ml/index.html
install.packages("RWeka")
library(RWeka)
getwd()
setwd("...")
system("mkdir ffdf")
options(fftempdir = "~/ffdf")
ckd <- read.arff("ckd_full.arff")

str(ckd)
levels(ckd$class)

ckd$class <- as.numeric(ckd$class)

library(ETLUtils)
ckd$class <- recoder(ckd$class, from = c(1,2), to=c(1,0))

table(ckd$class)

model0 <- glm(class ~ age + bp + bgr + bu + rbcc + wbcc + hemo, data = ckd, 
              family=binomial(link = "logit"), 
              na.action = na.omit)
model0
summary(model0)


library(ffbase)
ckd.ff <- as.ffdf(ckd)
dimnames(ckd.ff)

model0b <- glm(class ~ age + bp + bgr + bu + rbcc + wbcc + hemo, 
               data = ckd.ff, family=binomial(link = "logit"), 
               na.action = na.omit)
model0b
summary(model0b)


model1 <- bigglm.ffdf(class ~ age + bp + bgr + bu + rbcc + wbcc + hemo, 
                      data = ckd.ff, family=binomial(link = "logit"), 
                      na.action = na.exclude)
model1
summary(model1)

## Expanding the data with ffbase:
ckd.ff$id <- ffseq_len(nrow(ckd.ff))
system.time(big.ckd.ff <- expand.ffgrid(ckd.ff$id, ff(1:20000)))
colnames(big.ckd.ff) <- c("id","expand")
str(big.ckd.ff[1:20,])
head(big.ckd.ff)
system.time(big.ckd.ff <- merge.ffdf(big.ckd.ff, ckd.ff, 
                                     by.x="id", by.y="id", 
                                     all.x=TRUE, all.y=FALSE))
dim(big.ckd.ff)

system.time(model2 <- bigglm.ffdf(class ~ age + bp + bgr + bu + rbcc + wbcc + hemo, 
                                  data = big.ckd.ff, family=binomial(), 
                                  na.action = na.exclude))
model2
summary(model2)
summary(model2)$mat[2,]

system.time(model2 <- bigglm.ffdf(class ~ age + bp + bgr + bu + rbcc + wbcc + hemo, 
                                  data = big.ckd.ff, family=binomial(), 
                                  na.action = na.exclude, sandwich = TRUE, 
                                  chunksize = 20000, maxit = 40))

summary(model2)

system.time(model2 <- bigglm.ffdf(class ~ age + bp + bgr + bu + rbcc + wbcc + hemo, 
                                  data = big.ckd.ff, family=binomial(), 
                                  na.action = na.exclude, 
                                  chunksize = 100000, maxit = 20))

summary(model2)


# --- bigmemory package --- #

install.packages("bigmemory")
library(bigmemory)

need0 <- read.csv("need_puf14.csv", header = TRUE, sep = ",")
str(need0)

classes <- unlist(lapply(colnames(need0), function(x) {
  class(need0[,x])
}))

classes

ind <- which(classes=="factor")
for(i in ind) {need0[,i] <- as.integer(need0[, i])}

str(need0)

write.table(need0, "need_data.csv", sep = ",", 
            row.names = FALSE, col.names = TRUE)

need.mat <- read.big.matrix("need_data.csv", header = TRUE, 
                            sep = ",", type = "double",
                            backingfile = "need_data.bin",
                            descriptorfile = "need_data.desc")

need.mat

dim(need.mat)
dimnames(need.mat)
head(need.mat)
describe(need.mat)

# More basic big matrix information:
ncol(need.mat)
nrow(need.mat)

# Using bigtabulate and biganalytics packages:
install.packages("bigtabulate")
library(bigtabulate)

install.packages("biganalytics")
library(biganalytics)

bigtable(need.mat, c("PROP_AGE"))
bigtabulate(need.mat, c("PROP_AGE", "PROP_TYPE"))

summary(need.mat[, "Econs2012"])
sum1 <- bigtsummary(need.mat, c(39, 40), 
                    cols = 35, na.rm = TRUE)

sum1[1:length(sum1)]

## split-apply-combine operations:

need.bands <- bigsplit(need.mat, ccols = "EE_BAND", splitcol = "Econs2012")
sapply(need.bands, mean, na.rm=TRUE)

## multiple linear regression with bigmemory and biglm:

library(biglm)
regress1 <- bigglm.big.matrix(Econs2012~PROP_AGE + FLOOR_AREA_BAND 
                              + CWI_YEAR + BOILER_YEAR, 
                              data = need.mat, 
                              fc = c("PROP_AGE", "FLOOR_AREA_BAND"))

summary(regress1)
gc()

# Big algebra with bigalgebra package:
install.packages("bigalgebra")
library(bigalgebra)
help(package="bigalgebra")

# Synchronicity
install.packages("synchronicity")
library(synchronicity)
help(package="synchronicity")

## Importing data into a big.matrix object with bigpca:
install.packages("bigpca")
library(bigpca)
help(package="bigpca")

need.mat2 <- get.big.matrix("need_data.desc")
prv.big.matrix(need.mat2)

## Subsetting big matrices with bigpca:
need.subset <- big.select(need.mat2, select.cols = c(35, 37:50),
                          pref = "sub")
prv.big.matrix(need.subset)

### --- The end of Part 1 - Chapter 3 --- ###