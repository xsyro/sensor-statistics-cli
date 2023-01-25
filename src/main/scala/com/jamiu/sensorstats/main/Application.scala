package com.jamiu.sensorstats.main

import com.jamiu.sensorstats.ResourceHandler

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.io.{Source => ScalaIOSource}
import scala.language.implicitConversions

object Application extends App {

  sealed case class SensorStatistic(sensorId: String, humidity: String)

  private[this] implicit def parseToSensorStatistic(arrayOfFiles: Array[Vector[String]]): Array[Vector[SensorStatistic]] = arrayOfFiles.map(csvRecords => csvRecords.tail.map { lines =>
    val cols = lines.split(",").map(_.trim)
    SensorStatistic(sensorId = cols(0), humidity = cols(0))
  })

  implicit val fileReaderExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val sourceFuture: Future[Array[Vector[String]]] = ResourceHandler(args.headOption.orElse {
    println("Enter absolute path containing all the CSV files")
    ScalaIOSource.stdin.getLines().nextOption()
  })
    .filter(file => file.isFile && !file.isHidden)
    .filter(file => file.getName.substring(file.getName.length - 4).equals(".csv"))
    .printDirectory
    .readFileAsync { javaIoFile =>
      Future {
        val bfSource = ScalaIOSource.fromFile(javaIoFile)
        val lines = bfSource.getLines().toVector
        bfSource.close()
        lines
      }
    }







}
