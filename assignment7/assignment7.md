####
Pig analysis #1
####
```
A = LOAD '/shared/tweets2011.txt' USING PigStorage('\t') AS (uid:chararray, name:chararray, date:chararray, text:chararray);
B = FOREACH A GENERATE ToDate(date, 'EEE MMM dd HH:mm:ss Z YYYY','Etc/GMT') AS date;
C = FOREACH B GENERATE ToString(date, 'MM/dd HH') AS date;
D = group C by date;
E = foreach D generate group as term, COUNT(C) as count;
store E into 'hourly-counts-pig-all';
```
####
Pig analysis #2
####
```
A = LOAD '/shared/tweets2011.txt' USING PigStorage('\t') AS (uid:chararray, name:chararray, date:chararray, text:chararray);
X = FILTER A BY (text matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*');
B = FOREACH X GENERATE ToDate(date, 'EEE MMM dd HH:mm:ss Z YYYY','Etc/GMT') AS date;
C = FOREACH B GENERATE ToString(date, 'MM/dd HH') AS date;
D = group C by date;
E = foreach D generate group as term, COUNT(C) as count;
store E into 'hourly-counts-pig-egypt';
```
####
Spark analysis #1
####
```
val tweets = sc.textFile("/shared/tweets2011.txt")
val hourcount = tweets.map(l=>l.split("\t")).filter(l=>l.length==4)
val hour = hourcount.map(line=> line(2).substring(4,13)).map(h =>(h,1)).reduceByKey(_+_)
hour.saveAsTextFile("hourly-counts-spark-all")
```
####
Spark analysis #2
####
```
val tweets = sc.textFile("/shared/tweets2011.txt")
val hourcount = tweets.map(l=>l.split("\t")).filter(l=>l.length==4).filter(l=>l(3).matches(".*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*"))
val hour = hourcount.map(line=> line(2).substring(4,13)).map(h =>(h,1)).reduceByKey(_+_)
hour.saveAsTextFile("hourly-counts-spark-all")
```
