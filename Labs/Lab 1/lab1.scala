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

println("averge finish")

// 4. Determine the record(s) with the largest page size. If multiple records have the same size, list all of them.
val largePageSize = pageCountsCollection.filter(page => page.field4 == maxPages.field4)
largePageSize.collect.foreach(println)

println("largePageSize finish")

// 5. Determine the record with the largest page size again. But now, pick the most popular.
val largePageSizeAndPopular = largePageSize.reduce((acc,value) => { 
  if(acc.field3 < value.field3) value else acc})
println(largePageSizeAndPopular)

println("largePageSizeAndPopular finish")

// 6. Determine the record(s) with the largest page title. If multiple titles have the same length, list all of them.
val maxTitleLength = pageCountsCollection.reduce((acc,value) => { 
  if(acc.field2.length() < value.field2.length()) value else acc})

println("largePageTitle start")
val largePageTitle = pageCountsCollection.filter(page => page.field2.length() == maxTitleLength.field2.length())
largePageTitle.collect.foreach(println)

//7. Use the results of Question 3, and create a new RDD with the records that have greater page size than the average.
val pagesWithAboveAverageSizeCollection = pageCountsCollection.filter(page => page.field4.toLong > average.toLong)
// pagesWithAboveAverageSizeCollection.collect.foreach(println)

val pagesWithAboveAverageSizeRDD = sc.parallelize(Seq(pagesWithAboveAverageSizeCollection))
// pagesWithAboveAverageSizeRDD.collect.foreach(println)

// 8. Compute the total number of pageviews for each project (as the schema shows, the first field of each record contains the project code).
val projectViewCollection = pageCountsCollection.map(page => (page.field1, page.field3.toInt))
// projectViewCollection.take(15).foreach(println)
val projectTotalViews = projectViewCollection.reduceByKey((x,y)=>x+y)
// 9. Report the 10 most popular pageviews of all projects, sorted by the total number of hits.
val projectTotalViewsSorted = projectTotalViews.sortBy(_._2, false) 
projectTotalViewsSorted.take(10).foreach(println)

// 10. Determine the number of page titles that start with the article "The". How many of those page titles are not part of the English project (Pages that are part of the English project have "en" as the first field)?
var pagesThatStartWithThe = pageCountsCollection.filter(page => page.field2.startsWith("The"))
var pagesThatStartWithTheCount = pagesThatStartWithThe.count()
// pagesThatStartWithThe.take(3).foreach(println)
println(pagesThatStartWithThe)

var nonEnglishPagesThatStartWithThe = pagesThatStartWithThe.filter(page => !page.field1.equals("en"))
var nonEnglishPagesThatStartWithTheCount = nonEnglishPagesThatStartWithThe.count()
// pagesThatStartWithThe.take(3).foreach(println)
println(nonEnglishPagesThatStartWithTheCount)

// 11. Determine the percentage of pages that have only received a single page view in this one hour of log data.
val pagesWithSingleView = pageCountsCollection.filter(page => page.field3.toInt == 1).count()
println("pagesWithSingleView" + pagesWithSingleView)
val pagesWithSingleViewPercentage: Double = (pagesWithSingleView.toDouble/totalNumbeOfRecords.toDouble)*100
println("pagesWithSingleViewPercentage" + pagesWithSingleViewPercentage + "%")

// 13. Determine the number of unique terms appearing in the page titles. Note that in page titles, terms are delimited by "_" instead of a white space. You can use any number of normalization steps (e.g., lowercasing, removal of non-alphanumeric characters).
val pageTerms = pageCountsCollection.map(page => page.field2.toLowerCase().replaceAll("[^a-zA-Z0-9]", "_"))
val pageTermsCollection = pageTerms.map(page => page.split("_"))