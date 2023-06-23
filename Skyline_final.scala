
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.io.FileUtils
import org.apache.log4j._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks._
import scala.annotation.tailrec
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.parsing.json.JSON

object Skyline {
  def findCell(divisionNum: Int, dimensions: Int, min: RDD[List[Double]], max: RDD[List[Double]]): ListBuffer[ListBuffer[Double]] = {
    val startTimeBound = System.nanoTime()
    var boundaries = ListBuffer[ListBuffer[Double]]()
    for (i <- 0 until dimensions) {
      val maxDimension = max.map(_.zipWithIndex.filter(_._2 == i).map(_._1)).take(1)(0).head //min element of current column
      val minDimension = min.map(_.zipWithIndex.filter(_._2 == i).map(_._1)).take(1)(0).head  //min element of current column
      val interval = (maxDimension - minDimension) / divisionNum
      var boundariesSeq = ListBuffer[Double]()
      var previous = minDimension
      var counter = 0
      //calculate boundaries of each cell
      //iterate until max element
      while (previous <= maxDimension) {
        if (counter == 0) {
          boundariesSeq += previous
        } else {
          boundariesSeq += previous + interval
          previous = previous + interval
        }
        counter += 1
      }
      boundaries = boundaries :+ boundariesSeq //append boundary to list
    }

    val endTimeBondary = System.nanoTime()
    val elapsedTimeBF = (endTimeBondary - startTimeBound) / 1000000000.0
    println("Find cell " + elapsedTimeBF + " seconds")
    boundaries //return list for boundaries
  }

  def mapPointToCell[T](rddDataset: Iterator[List[Double]], boundariesList: ListBuffer[ListBuffer[Double]]): Iterator[List[Double]] = {
    val cellBound = rddDataset.map(point => {
      point.zipWithIndex
        .map(dimension => {
          var bound = boundariesList(dimension._2).min
          val boundList = boundariesList(dimension._2).filter(bound => bound <= dimension._1)
          if (boundList.nonEmpty) bound = boundList.max
          List[Double](dimension._1, bound)
        }).transpose
    }).toList.groupBy(x => x(1))

    val filteredBounds = cellBound.keys
    val finalBounds = cellBound.keys.filter(x => filteredBounds.forall(y => x.zip(y).exists { case (a, b) => a <= b }))

    cellBound.filter(key => finalBounds.toList.contains(key._1))
      .values.flatMap(_.flatten)
      .filter(point => !finalBounds.toList.contains(point)).iterator
  }

  def transformList(D: List[List[Double]]): List[List[Double]] = {
    //Add score on each tuple of D. Score is the monotonic function ln(x+1)
    val transformedList = D.map { l =>
      val logSum = l.map(x => math.log(x + 1)).sum
      l :+ logSum
    }
    //Sort 𝐷 according to their score
    val sortedList = transformedList.sortBy((- _.last))
    val finalList = sortedList.map(_.init)
    finalList
  }


  def addScoreAndSort(D: Iterator[List[Double]]): Iterator[List[Double]] = {
    //Add score on each tuple of D. Score is the monotonic function ln(x+1)
    val score = D.map { l =>
      val score = l.sum
      l :+ score
    }

    //Sort 𝐷 according to their score
    val sortedList = score.toList.sortBy(_.last)
    val finalList = sortedList.map(_.init)
    finalList.toIterator
  }

  def SFSkylineCalculation(rdd: Iterator[List[Double]]): Iterator[List[Double]] = {

    var skyline = ArrayBuffer[List[Double]]()
    val dataset = rdd.toList

    skyline += dataset.head

    for(i <- 1 until dataset.length) {
      var toBeAdded = true
      var j = 0
      breakable{
        while(j < skyline.length){
          if(dominates(dataset(i), skyline(j))){
            skyline.remove(j)
            j -= 1
          }
          else if (dominates(skyline(j), dataset(i))){
            toBeAdded = false
            break()
          }
          j += 1
        }}
      if (toBeAdded) {
        skyline += dataset(i)
      }
    }
    skyline.toIterator
  }

  def addScoreAndCalculate(x: Iterator[List[Double]]):Iterator[List[Double]]={
    val ysort = addScoreAndSort(x)
    val result = SFSkylineCalculation(ysort)
    result
  }

  def calculatePartition(previousSkylines: ArrayBuffer[List[Double]], enteredPartition: Iterator[List[Double]]): Iterator[List[Double]]= {

    var wasEmpty=false
    val array = enteredPartition.toList
    if(previousSkylines.isEmpty){
      previousSkylines += array.head
      wasEmpty=true
    }

    // For every skyline point of the enteredPartition,
    // check if it is dominated by or if it dominates any other previous skyline point from other partitions

    for (i <- 0 to array.length - 1) {
      var j = 0
      var breaked = false
      breakable {
        while (j < previousSkylines.length) {
          if (dominates(array(i), previousSkylines(j))) {
            previousSkylines.remove(j)
            j -= 1
          }
          else if (dominates(previousSkylines(j), array(i))) {
            breaked = true
            break()
          }
          if(wasEmpty & i==0)
          {
            breaked=true
            break()
          }
          j += 1
        }
      }
      if (!breaked) {
        previousSkylines += array(i)
      }
    }
    previousSkylines.toIterator
  }


  def dominates(p1: List[Double], p2: List[Double]): Boolean = {
    //check if 𝑝1. 𝑖 < 𝑝2. 𝑖 for at least one dimension i
    val booleanList = p1.zip(p2).map { case (x, y) => x < y }
    //check if (𝑝1. 𝑖 ≤ 𝑝2. 𝑖 for each dimension 𝑖) AND (𝑝1. 𝑖 < 𝑝2. 𝑖 for at least one dimension 𝑖)
    val newBooleanList = p1.zip(p2).map { case (x, y) => x <= y && booleanList.contains(true) }
    newBooleanList.forall(x => x)
  }

  def sortArrayDesc(array: RDD[(List[Double], Int)]): RDD[(List[Double], Int)] = {
    //This function sorts a RDD[(List[Double], Int)] by the integers
    array.sortBy(-_._2)
  }

  def dominanceScore(data: Iterator[List[Double]], dataToScore: List[List[Double]]): Iterator[(List[Double], Int)] = {
    // This function returns the points of interest dataToScore and their dominance scores.
    import org.apache.spark.SparkContext._

    val points = data.toList
    var domScores = List[Int]()
    //for every point d of interest, count the points from the dataset that d dominates
    for (d <- dataToScore) {
      domScores = domScores :+ points.count(p => dominates(d, p))
    }
    dataToScore.zip(domScores).toIterator
  }


  def topkDominating(rdd: RDD[List[Double]], K: Int, partitions: Int, skyline:  Broadcast[List[List[Double]]], iteration: Int,  sc: SparkContext): List[List[Double]] = {
    //This function returns the top-k dominating points of the dataset
    //It runs recursively the loop functio, until it reaches the k-th top point
    @tailrec // the @tailrec annotation is used to indicate that a function is a tail-recursive
    // function, and that the Scala compiler should optimize the function by replacing
    // the recursive call with a loop.
    def loop(rdd1: RDD[List[Double]], k: Int, topkPoints: List[List[Double]], skyline:  Broadcast[List[List[Double]]], iteration: Int): List[List[Double]] = {

      //if k = 0, no point is returned
      if (k == 0) topkPoints
      else {
        if(iteration == 1){
          //find dominance score among skyline points
          val sortedDomScores = sortArrayDesc(
            rdd1.mapPartitions(x => dominanceScore(x, skyline.value))
              .reduceByKey(_ + _)
              .sortBy(-_._2)
          ).persist(StorageLevel.MEMORY_AND_DISK)

          val file = new java.io.File("top_skyline_elements")
          if (file.exists()) {
            FileUtils.deleteDirectory(file) //if directory exists, remove it
          }
          sc.parallelize(sortedDomScores.take(k).map(_._1)).saveAsTextFile("top_skyline_elements") // take top-k from skyline

          val top = sortedDomScores.map(_._1).first() //take top-1 dominant points
          loop(rdd1.filter(_ != top), k - 1, topkPoints :+ top, null, 2) // run again
        }else{
          //find skyline excluding top-1 point
          val globalSkyline =
            rdd1.repartition(partitions)
              .mapPartitions(addScoreAndCalculate)
              .coalesce(1).mapPartitions(addScoreAndCalculate).collect().toList
          val broadcastSkyline = sc.broadcast(globalSkyline)
          //calculate dominance score of skyline points
          val domScores =
            rdd1
              .mapPartitions(x => dominanceScore(x, broadcastSkyline.value))
              .reduceByKey(_ + _).sortBy(-_._2)
          val top = sortArrayDesc(domScores).map(_._1).first() //take top-1
          loop(rdd1.filter(_ != top), k - 1, topkPoints :+ top, null, 2) // then run again
        }
      }
    }

    loop(rdd, K, List[List[Double]](), skyline, iteration)
  }

  def findMin(rdd: Iterator[List[Double]]): Iterator[List[Double]] = {
    //min and max element of each column
    var localMin = ArrayBuffer[List[Double]]()
    localMin +=  rdd.toList.transpose.map(_.min)
    localMin.iterator
  }


  def findMax(rdd: Iterator[List[Double]]): Iterator[List[Double]] = {
    //min and max element of each column
    var localMax = ArrayBuffer[List[Double]]()
    localMax += rdd.toList.transpose.map(_.max)
    localMax.iterator
  }


  def main(args: Array[String]): Unit = {

    println("***********************************************************************************************")
    println("Hi, this is the Skyline application for Spark.")
    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    // Create spark configuration
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Skyline")

    // Create spark context
    val sc = new SparkContext(sparkConf)  // create spark context

    val currentDir = System.getProperty("user.dir") // get the current directory
    val source: String = Source.fromFile("params.json").getLines.mkString
    val json_data = JSON.parseFull(source)

    val dataset = json_data.get.asInstanceOf[Map[String, Any]]("fileName").asInstanceOf[String] //get dataset file name
    val divisionNum = json_data.get.asInstanceOf[Map[String, Any]]("divisionNum").asInstanceOf[Double].toInt //number of divisions
    var num_of_partitions = json_data.get.asInstanceOf[Map[String, Any]]("num_of_partitions").asInstanceOf[Double].toInt //number of partitions
    val topKpoints = json_data.get.asInstanceOf[Map[String, Any]]("topKpoints").asInstanceOf[Double].toInt //get input of top-k
    val datasetRDD = sc.textFile(dataset).map(x => x.split(",")).map(x => x.map(y => y.toDouble).toList) //load csv
    val dimensions = datasetRDD.take(1).toList.head.length //number of dimensions

    // ********************  TASK 1 ****************
    val startTimeGrid = System.nanoTime() //start grid time

    val datasetPartitioned = datasetRDD.repartition(num_of_partitions) //separate dataset into partitions
    val partialMin = datasetPartitioned.mapPartitionsWithIndex { (partitionIndex, partition) =>
      findMin(partition) //find min for each partition
    }
    val globalMin = partialMin.coalesce(1).mapPartitions(findMin) //find global min
    val partialMax = datasetPartitioned.mapPartitionsWithIndex { (partitionIndex, partition) =>
      findMax(partition) //find max for each partition
    }
    val globalMax = partialMax.coalesce(1).mapPartitions(findMax) //find global max

    val cells = findCell(divisionNum, dimensions, globalMin, globalMax) //find boundaries of each cell

    val partialMapPoint = datasetPartitioned.mapPartitionsWithIndex { (partitionIndex, partition) =>
      mapPointToCell(partition,cells) //matching point to cell
    }
    val dataOnMinCells = partialMapPoint.coalesce(1).persist(StorageLevel.MEMORY_AND_DISK) //find points that belongs to cells of minimum boundaries
    val dataCellsCount = dataOnMinCells.count() //count points from grid partitioning algorithm

    val endTimeGrid = System.nanoTime() //end gird time
    val elapsedTimeGrid = (endTimeGrid - startTimeGrid) / 1000000000.0

    if (dataCellsCount < num_of_partitions) {
      num_of_partitions = num_of_partitions / 5 //reduce number of partitions in case of small dataset
    }

    // Rdd create timer
    val rddCreateTime: Long = System.nanoTime //start ALS time with grid partitioning

    // All-Local Skyline (Task 1)
    // - perform parallel skyline calculation for each partition
    // - collect to driver and calculate the final skyline

    //separate dataset into partitions and measure score for each point
    val rdd1: RDD[List[Double]] = dataOnMinCells.repartition(num_of_partitions).mapPartitions(addScoreAndCalculate)

    val partitionSkylines: ArrayBuffer[List[Double]] = ArrayBuffer[List[Double]]()
    rdd1.collect().foreach(x => calculatePartition(partitionSkylines, Iterator(x))) //find global skyline from all partitions
    println("Skyline points = " + partitionSkylines.length)
    println("***************number of grid points: " + dataCellsCount)

    // skyline (Task 1) timer
    val skylineTime: Long = System.nanoTime //end ALS time with grid partitioning
    val stage2 = (skylineTime-rddCreateTime).asInstanceOf[Double] / 1000000000.0
    val totalTime = stage2 + elapsedTimeGrid

    println("Grid Duration = " + elapsedTimeGrid + " seconds")
    println("Skyline Duration = " + stage2 + " seconds")
    println("Total Duration of grid partitioning combined with ALS= " + totalTime + " seconds")

    val rddCreateTime2: Long = System.nanoTime // Rdd create timer for ALS

    //calculate ALS without gird partitioning
    val rdd2: RDD[List[Double]] = datasetRDD.repartition(num_of_partitions).mapPartitions(addScoreAndCalculate)
    val partitionSkylinesALS: ArrayBuffer[List[Double]] = ArrayBuffer[List[Double]]()
    rdd2.collect().foreach(x => calculatePartition(partitionSkylinesALS, Iterator(x)))

    val skylineTime2: Long = System.nanoTime // Rdd end timer for ALS
    val ALStime = (skylineTime2-rddCreateTime2).asInstanceOf[Double] / 1000000000.0
    println("Skyline points = " + partitionSkylinesALS.length)
    println("ALS Skyline Duration = " + ALStime + " seconds")

    val broadcastSkyline = sc.broadcast(partitionSkylines.toList)

    // TASK 2
    val startTimeTask2 = System.nanoTime() //start timer for top-k calculation
    val topPoints = topkDominating(datasetRDD, topKpoints, num_of_partitions, broadcastSkyline, 1, sc) //calculate top-k dominant points
    println("***********************************************************************************************")
    println("partitions:"+num_of_partitions+ " ,top Points: "+topPoints)
    val endTimeTask2 = System.nanoTime()  //end timer for top-k calculation
    val elapsedTimeTask2 = (endTimeTask2 - startTimeTask2) / 1000000000.0
    println("Duration for Task 2 = " + elapsedTimeTask2 + " seconds")
    println("***********************************************************************************************")

    //TASK 3
    val startTimeTask3 = System.nanoTime() //start timer for top-k skyline points
    val topKFile = "file://" + currentDir + "/top_skyline_elements"
    val topSkylinePoints = sc.textFile(topKFile).collect().toList //read file including top-k points from skyline
    println("***********************************************************************************************")
    println("partitions:"+num_of_partitions+ " ,top Points: "+topSkylinePoints)
    val endTimeTask3 = System.nanoTime() //end timer for top-k skyline points
    val elapsedTimeTask3 = (endTimeTask3 - startTimeTask3) / 1000000000.0
    println("Duration for Task 3 = " + elapsedTimeTask3 + " seconds")
    println("***********************************************************************************************")
    sc.stop()
  }
}

