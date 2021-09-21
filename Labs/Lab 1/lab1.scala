%scala
val pagecounts = sc.textFile("/FileStore/tables/pagecounts_20160101_000000_parsed-1.out")

//1. Create a case class called Log using the four field names of the dataset.
case class Log(field1: String, field2: String, field3: String, field4: String)

//2. Create a function that takes a string, split it by white space and converts it into a log object.
//3. Create a function that takes an RDD[String] and returns an RDD[Log]
val pageCountsCollection = pagecounts.map(x => Log(x.split(" ")(0), x.split(" ")(1), x.split(" ")(2), x.split(" ")(3)))

// 1. Retrieve the first 15 records and print out the result.
pageCountsCollection.take(15).foreach(println)
// 2. Determine the number of records the dataset has in total.
val totalNumbeOfRecords = pageCountsCollection.count()
println(totalNumbeOfRecords)
// 3. Compute the min, max, and average page size.

val maxPages = pageCountsCollection.reduce((acc,value) => { 
  if(acc.field4 < value.field4) value else acc})
println(maxPages.field4)

val minPages = pageCountsCollection.reduce((acc,value) => { 
  if(acc.field4 > value.field4) value else acc})
println(minPages.field4)

val average = pageCountsCollection.map(_.field4.toLong).sum/totalNumbeOfRecords.toLong
println(average)

val largePageSize = pageCountsCollection.filter(page => page.field4 == maxPages.field4)
largePageSize.collect.foreach(println)

val largePageSizeAndPopular = largePageSize.reduce((acc,value) => { 
  if(acc.field3 < value.field3) value else acc})
println(largePageSizeAndPopular)