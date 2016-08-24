### --- Chapter 3 --- ###

### --- Part 2 - Parallel and faster R --- ###

library(bigmemory)
library(biganalytics)

need.big.bm <- attach.resource("need_big.desc")
system.time(meanbig.bm1 <- colmean(need.big.bm, na.rm = TRUE))
meanbig.bm1

# apply() example with big.matrix object:
meanbig.bm2 <- apply(need.big.bm, 2, mean, na.rm=TRUE)
meanbig.bm2

# for() loop example with ffdf object:
meanbig.ff <- list()
for(i in 1:ncol(need.big.ff)) {
  meanbig.ff[[i]] <- mean.ff(need.big.ff[[i]], na.rm=TRUE)
}
meanbig.ff

# apply() and for() loop examples on a data.frame:
for(i in 1:ncol(need.big.df)) {
  x1[i] <- mean(need.big.df[,i], na.rm = TRUE)}
x1

x2 <- colMeans(need.big.df, na.rm = TRUE)
x2

x3 <- sapply(need.big.df, mean, na.rm=TRUE)
x3

# parallel package example:

library(parallel)
help(package="parallel")

detectCores()
cl <- makeCluster(3, type = "SOCK")
cl

meanbig <- clusterApply(cl, need.big.df, fun=mean, na.rm=TRUE)
meanbig

meanbig2 <- parSapply(cl, need.big.df, FUN = mean, na.rm=TRUE)
meanbig2

meanbig3 <- mclapply(need.big.df, FUN = mean, na.rm = TRUE, mc.cores = 1)
meanbig3

stopCluster(cl)

# foreach package example:

install.packages("foreach")
library(foreach)

install.packages("doParallel")
library(doParallel)

library(parallel)

cl <- makeCluster(3, type = "SOCK")
registerDoParallel(cl)

x4 <- foreach(i = 1:ncol(need.big.df)) %dopar% mean(need.big.df[,i], na.rm=TRUE)
x4

stopCluster(cl)

x5 <- foreach(i = 1:ncol(need.big.df), .combine = "c") %do% mean(need.big.df[,i], na.rm=TRUE)
x5

### --- data.table tutorial --- ###

install.packages("data.table")
library(data.table)
help(package = "data.table")
rm(list=ls())
gc()
getwd()
setwd("...")

## Importing the data:
system.time(flightsDT <- fread("flights_1314.txt", stringsAsFactors = TRUE))

str(flightsDT)
gc()

## Subsetting the data - selecting rows (i):

subset1.DT <- flightsDT[
  YEAR == 2013L & DEP_TIME >= 1200L & DEP_TIME < 1700L,
  ]

str(subset1.DT)


subset2.DT <- flightsDT[
  MONTH == 12L,
  .(TotDelay = ARR_DELAY - DEP_DELAY,
    AvgDepDelay = mean(DEP_DELAY, na.rm = TRUE)),
  by = .(ORIGIN_STATE_NM)
  ]

subset2.DT
str(subset2.DT)

## Subsetting the data - selecting columns (j):

subset3.DT <- flightsDT[, .(MONTH, DEST)]
str(subset3.DT)

## Fast aggregations (j by group):

agg1.DT <- flightsDT[, .(SumCancel = sum(CANCELLED),
                        MeanArrDelay = mean(ARR_DELAY, na.rm = TRUE)),
                        by = .(ORIGIN_CITY_NAME)
                        ]

str(agg1.DT)
agg1.DT

## Order the aggregated data.table:

agg1.DT[order(-MeanArrDelay, -SumCancel)]

## Internal order optimisation:

system.time(flightsDT[base::order(-ARR_DELAY)])

system.time(flightsDT[order(-ARR_DELAY)])

## More aggregations:
system.time(agg2.DT <- flightsDT[, .N, by = ORIGIN_STATE_NM])
agg2.DT

# Compare with base and data.frame approach:

system.time(agg2.df <- as.data.frame(table(flightsDT$ORIGIN_STATE_NM)))
agg2.df

## Chaining:

system.time(agg3.DT <- flightsDT[, .N, by = ORIGIN_STATE_NM]
            [order(-N)])

# More complex aggregations:

system.time(agg4.DT <- flightsDT[MONTH == 12L,
                                 lapply(.SD, mean, na.rm = TRUE),
                                 by = .(ORIGIN_STATE_NM, 
                                        DEST_STATE_NM, 
                                        DAY_OF_WEEK),
                                 .SDcols = c("DEP_DELAY", "ARR_DELAY")]
            [order(DAY_OF_WEEK, -DEP_DELAY, -ARR_DELAY)])

head(agg4.DT, n=5)


# Creating functions for data.table:
## a.) We want to calculate TOT_DELAY for each flight and add this variable to our flightsDT
## b.) We want to calculate MEAN_DELAY for each day of month.

delay <- function(DT) {
  DT[, TOT_DELAY := ARR_DELAY - DEP_DELAY]
  DT[, .(MEAN_DELAY = mean(TOT_DELAY, na.rm = TRUE)), by = DAY_OF_MONTH]
}

delay.DT <- delay(flightsDT)

names(flightsDT)
head(delay.DT)

# Pivot tables using dcast.data.table:
agg5.DT <- dcast.data.table(flightsDT,
                            UNIQUE_CARRIER~MONTH,
                            fun.aggregate = mean,
                            value.var = "TOT_DELAY",
                            na.rm=TRUE)

agg5.DT


### --- The end of Part 2 - Chapter 3 --- ###