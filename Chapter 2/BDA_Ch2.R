### --- Revisiting R basics - Chapter 2 --- ###

#Set the working directory path to where you store the data:
setwd("...")
getwd()

#Set repositories:
setRepositories(addURLs = c(CRAN = "https://cran.r-project.org/")) 
getOption('repos')

#R data structures
##Vectors:
vector1 <- rnorm(10)
vector1

set.seed(123)
vector2 <- rnorm(10, mean=3, sd=2)
vector2

vector3 <- c(6, 8, 7, 1, 2, 3, 9, 6, 7, 6)
vector3

length(vector3)
class(vector3)
mode(vector3)

vector4 <- c("poor", "good", "good", "average", "average", "good", "poor", "good", "average", "good")
vector4
class(vector4)
mode(vector4)
levels(vector4)

vector4 <- factor(vector4, levels = c("poor", "average", "good"))
vector4

vector4.ord <- ordered(vector4, levels = c("good", "average", "poor"))
vector4.ord

class(vector4.ord)
mode(vector4.ord)
levels(vector4.ord)
str(vector4.ord)

vector5 <- c(TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE)
vector5
length(vector5)

##Scalars:
a1 <- 5
a1

a2 <- 4
a2

a3 <- a1 + a2
a3

#Matrices:
y <- matrix(1:20, nrow=5, ncol=4) #the colon operator is to generate a sequence of numbers
y

rows <- c("R1", "R2", "R3", "R4", "R5")
columns <- c("C1", "C2", "C3", "C4")
z <- matrix(1:20, nrow=5, ncol=4, byrow=TRUE, dimnames=list(rows, columns))
z

y[4,2]
y[,3]

z[c(2, 3, 5), 1]

##Arrays:
array1 <- array(1:20, dim=c(2,2,5)) #dim - a numeric vector giving the maximal index for each dimension = 1 row, 2 columns, 3 dimensions
array1

dim(array1)
array1[2, 1, 4]
which(array1==11, arr.ind=TRUE)

##Data frames:
subjectID <- c(1:10)
age <- c(37,23,42,25,22,25,48,19,22,38)
gender <- c("male", "male", "male", "male", "male", "female", "female", "female", "female", "female")
lifesat <- c(9,7,8,10,4,10,8,7,8,9)
health <- c("good", "average", "average", "good", "poor", "average", "good", "poor", "average", "good")
paid <- c(T, F, F, T, T, T, F, F, F, T)
dataset <- data.frame(subjectID, age, gender, lifesat, health, paid)
dataset

str(dataset)

dataset[,2:4] #or
dataset[, c("age", "gender", "lifesat")]

subset(dataset[c("age", "gender", "lifesat")])
subset(dataset, age > 30 & lifesat >= 8)

subset(dataset, paid==TRUE, select=c("age", "gender"))

##Lists:
simple.vector1 <- c(1, 29, 21, 3, 4, 55)
simple.matrix <- matrix(1:24, nrow=4, ncol=6, byrow=TRUE)
simple.scalar1 <- 5
simple.scalar2 <- "The List"
simple.vector2 <- c("easy", "moderate", "difficult")

simple.list <- list(name=simple.scalar2, matrix=simple.matrix, vector=simple.vector1, scalar=simple.scalar1, difficulty=simple.vector2)
simple.list
str(simple.list)

simple.list[[2]][1,3]

#Exporting R data objects:
ls()

save.image(file = "workspace.RData")
save.image(file = "workspace2.RData", compress = "xz")
save(dataset, simple.list, file = "two_objects.RData")

rm(list=ls())
load("workspace2.RData")
dump(ls(), file = "dump.R", append = FALSE)
dput(dataset, file = "dput.txt")

cat(age, file="age.txt", sep=",", fill=TRUE, labels=NULL, append=TRUE)
cat(age, file="age.csv", sep=",", fill=TRUE, labels=NULL, append=TRUE)  

write(age, file="agedata.csv", ncolumns=2, append=TRUE, sep=",")
write(y, file="matrix_y.tab", ncolumns=2, append=FALSE, sep="\t")

install.packages("MASS")
library(MASS)
write.matrix(y, file="ymatrix.txt", sep=",")

write.table(dataset, file="dataset1.txt", append=TRUE, sep=",", na="NA", col.names=TRUE, row.names=FALSE, dec=".") #in this case as the subjectID has the same values as the row.names it's better to assign FALSE to row.names to deduplicate. In write.table() function you can also set fileEncoding and qmethod (quoting method) arguments

install.packages("WriteXLS")
library(WriteXLS)
WriteXLS("dataset", "dataset1.xlsx", SheetNames=NULL, 
         row.names=FALSE, col.names=TRUE, AdjWidth=TRUE, 
         envir=parent.frame()) 

install.packages("openxlsx")
library(openxlsx)
write.xlsx(simple.list, file = "simple_list.xlsx")

library(foreign)
write.foreign(dataset, "datafile.txt", "codefile.txt", package="SPSS")

install.packages("rio")
library(rio)

export(dataset, format = "stata")
export(dataset, "dataset1.csv", col.names = TRUE, na = "NA")


### --- Applied Data Science in R - Chapter 2 --- ###

#Importing data from different formats:
#setwd("...") #set working directory to the location where you saved the data
library(foreign)
grel <- read.spss("NILT2012GR.sav", to.data.frame=TRUE)
head(grel, n=5)
str(grel)

#Store a subset of the dataset to the object named grel.data 
#with the following variables only:

#serial - 1 #serial number of respondent
#househld - 2 #How many people are there in your household?
#rage - 3 #Age of respondent
#rsex - 5 #Gender of respondent
#nkids - 7#Number of children aged less than 18 living in the household
#nelderly - 8 #Number of people aged 65 or more living in the household
#rmarstat - 10 #Marital/civil partnership status
#placeliv - 12 #Would you describe the place where you live as (1) a big city, (2) the suburbs of a big city, (3) a small city or town, (4) a country village, (5) a farm or country home
#eqnow1 - 16 #Are Catholics generally treated unfairly?
#eqnow2 - 17 #Are Protestants generally treated unfairly?
#eqnow3 - 18 #Are Gays/Lesbians/Bisexuals generally treated unfairly?
#eqnow4 - 19 #Are Disabled people generally treated unfairly?
#tenshort - 38 #Housing tenure status
#highqual - 39 #Highest educational qualification
#work - 41 #Work status
#ansseca - 47 #Social class
#religcat - 52 #Do you regard yourself as belonging to any particular religion?
#chattnd2 - 55 #How often do you attend religious services?
#persinc2 - 60 #Personal income before tax and NI deductions?
#orient - 64 #Sexual orientation
#ruhappy - 66 #Are you happy?

#contegrp - 76 #Number of ethnic groups respondent has regular contact with?
#uprejmeg - 77 #How prejudiced are you againts people of minority ethnic communities?
#umworker - 80 #Are you a migrant worker?
#mil10yrs - 81 #How good the settlement of migrants in the last 10 years has been for NI?
#miecono - 82 #How good for NI's economy that migrants come from other countries?
#micultur - 83 #How much has the cultural life in NI become enriched by migrants?
#target1a - 105 #Whether agree that NI is a normal civic society in which all individuals are equal etc?
#target2a - 106 #... NI is a place free from displays of sectarian aggression?
#target3a - 107 #...towns and city centres in NI are safe and welcoming?
#target4a - 108 #...schools in NI are effective at preparing pupils for life in a diverse society?
#target5a - 109 #...schools in NI are effective at encouraging understanding of the complexity of our history?
#target6a - 110 #...the government is actively encouraging integrated schools?
#target7a - 111 #...the government is actively encouraging schools of different religions to mix with each other by sharing facilities?
#target8a - 112 #...the government is actively encouraging shared communities where people of all backgrounds can live, work, learn and play together?

grel.data <- subset(grel[, c(1:3, 5, 7:8, 10, 12, 16:19, 38:39, 41, 47, 52, 55, 60, 64, 66, 76:77, 80:83, 105:112)])
str(grel.data)

#Exploratory Data Analysis:
library(psych)
describe(grel.data)

mean(grel.data$rage, na.rm=TRUE)
median(grel.data$rage, na.rm=TRUE)
library(modeest)
mlv(grel.data$rage, method="mfv", na.rm=TRUE)

var(grel.data$rage, na.rm=TRUE) #variance
sd(grel.data$rage, na.rm=TRUE) #standard deviation
range(grel.data$rage, na.rm=TRUE) #range

cent.tend <- function(data) {
  library(modeest)
  data <- as.numeric(data)
  m <- mean(data, na.rm=TRUE)
  me <- median(data, na.rm=TRUE)
  mo <- mlv(data, method="mfv", na.rm=TRUE)[1]
  stats <- data.frame(c(m, me, mo), row.names="Totals:")
  names(stats)[1:3] <- c("Mean", "Median", "Mode")
  return(stats)
}

cent.tend(grel.data$rage)

summary(grel.data[c("rage", "persinc2")])

#Data aggregations and contingency tables:
aggregate(grel.data[c("rage", "persinc2", "househld")], by=list(rsex=grel.data$rsex), FUN=mean, na.rm=TRUE)

stats <- function(x, na.omit=TRUE) {
  if(na.omit)
    x <- x[!is.na(x)]
  n <- length(x)
  m <- mean(x)
  s <- sd(x)
  r <- range(x)
  return(c(n=n, mean=m, stdev=s, range=r))
}

library(doBy)
summaryBy(rage+persinc2+househld~rsex, data=grel.data, FUN=stats)

library(psych)
describeBy(grel.data[c("rage", "persinc2", "househld")], grel.data$rsex) #describeBy() does not allow to specify an arbitrary function; you can write a list of grouping variables; if you prefer a matrix output (instead of a list) add parameter mat=TRUE, e.g.:
describeBy(grel.data[c("rage", "persinc2", "househld")], grel.data$rsex, mat=TRUE)

table(grel.data$househld)
table(grel.data$uprejmeg) 

library(Hmisc)
Hmisc::describe(grel.data)

attach(grel.data)
table(uprejmeg, househld)
detach(grel.data)

xtabs(~uprejmeg+househld+rsex, data=grel.data)
xTab <- xtabs(~uprejmeg+househld+rsex, data=grel.data)
ftable(xTab)

#Hypothesis testing and statistical inference
##Tests of differences
###Independent t-test example:
grel.data$ruhappy <- as.numeric(grel.data$ruhappy)
table(grel.data$ruhappy)

library(car)
grel.data$ruhappy <- recode(grel.data$ruhappy, "5=NA")
table(grel.data$ruhappy)

aggregate(grel.data$ruhappy, by=list(rsex=grel.data$rsex), FUN=mean, na.rm=TRUE)

t.test(ruhappy ~ rsex, data=grel.data, alternative="two.sided")

library(doBy)
stats <- function(x, na.omit=TRUE){
  if(na.omit)
    x <- x[!is.na(x)]
  m <- mean(x)
  n <- length(x)
  s <- sd(x)
  return(c(n=n, mean=m, stdev=s))
}
sum.By <- summaryBy(ruhappy~rsex, data=grel.data, FUN=stats)
sum.By

attach(sum.By)
n.harm <- (2*ruhappy.n[1]*ruhappy.n[2])/(ruhappy.n[1]+ruhappy.n[2])
detach(sum.By)
n.harm

pooledsd.ruhappy <- sd(grel.data$ruhappy, na.rm=TRUE)
pooledsd.ruhappy

power.t.test(n=round(n.harm, 0), delta=sum.By$ruhappy.mean[1]-sum.By$ruhappy.mean[2], sd=pooledsd.ruhappy, sig.level=.03843, type="two.sample", alternative="two.sided")

d <- (sum.By$ruhappy.mean[1]-sum.By$ruhappy.mean[2])/pooledsd.ruhappy #effect size = means difference divided by pooled standard deviation
d

###ANOVA example:
levels(grel.data$placeliv)

library(car)
qqPlot(lm(ruhappy ~ placeliv, data=grel.data), simulate=TRUE, main="Q-Q Plot", labels=FALSE) #The data fall quite well within the 95% Confidence Intervals - this suggests that the normality assumption has been met. 
bartlett.test(ruhappy~placeliv, data=grel.data)

ruhappy.means <- aggregate(grel.data$ruhappy, by=list(grel.data$placeliv), FUN=mean, na.rm=TRUE)
ruhappy.means

fit <- aov(ruhappy~placeliv, data=grel.data)
summary(fit)

library(gplots)
plotmeans(grel.data$ruhappy~grel.data$placeliv, xlab="Place of living", ylab="Happiness", main="Means Plot with 95% CI")

TukeyHSD(fit)

opar <- par(no.readonly = TRUE)
par(las=2)
par(mar=c(6,27,5,2)) #adjust margin areas according to your settings to fit all labels
plot(TukeyHSD(fit))
par(opar)


##Tests of relationships
###Pearson's r correlations example:
grel.data$miecono <- as.numeric(grel.data$miecono)

library(car)
grel.data$miecono <- recode(grel.data$miecono, "1=0;2=1;3=2;4=3;5=4;6=5;7=6;8=7;9=8;10=9;11=10")
table(grel.data$miecono)

cov(grel.data$ruhappy, grel.data$miecono, method="pearson", use = "complete.obs")

cor.test(grel.data$ruhappy, grel.data$miecono, alternative="two.sided", method="pearson")

N.compl <- sum(complete.cases(grel.data$ruhappy, grel.data$miecono))
N.compl
cor.grel <- cor(grel.data$ruhappy, grel.data$miecono, method="pearson", use = "complete.obs")
cor.grel

adjusted.cor <- sqrt(1 - (((1 - (cor.grel^2))*(N.compl - 1))/(N.compl - 2)))
adjusted.cor

library(pwr)
pwr.cor <- pwr.r.test(n=N.compl, r=cor.grel, sig.level=0.0000006649, alternative="two.sided")
pwr.cor

###Multiple regression example:
reg.data <- subset(grel.data, select = c(ruhappy, rage, persinc2, househld, contegrp))
str(reg.data)

library(psych)
corr.test(reg.data[1:5], reg.data[1:5], use="complete", method="pearson", alpha=.05)

library(car)
scatterplotMatrix(reg.data[1:5], spread=FALSE, lty.smooth=2, main="Scatterplot Matrix")

library(corrgram)
corrgram(reg.data[1:5], order=TRUE, lower.panel=panel.shade, upper.panel=panel.pie, text.panel=panel.txt) #for the pie graphs - the filled portion of the pie indicates the magnitude of the correlation; for the panel.shade - the depth of the shading indicates the magnitude of the correlation

attach(reg.data)
regress1 <- lm(ruhappy~rage+persinc2+househld+contegrp)
detach(reg.data)
regress1
summary(regress1)

### --- The end of Chapter 2 --- ###