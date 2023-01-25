package com.jamiu.sensorstats.main

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.jamiu.sensorstats.ResourceHandler

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.io.{Source => ScalaIOSource}
import scala.language.implicitConversions

object Application extends App {

  sealed case class SensorStatistic(sensorId: String, humidity: String)

  implicit val fileReaderExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val futureCsvRecordFiles: Future[Array[Vector[String]]] = ResourceHandler(args.headOption.orElse {
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


  /**
   * Akka Stream
   */
  implicit val actorSystem = ActorSystem("sensor-statistics")

  val source = Source.future(futureCsvRecordFiles.map(_.flatten.tail.toVector)).flatMapConcat(iteration => Source.fromIterator(() => iteration.iterator))
  val flow = Flow[String].map { s =>
    val cols = s.split(",").map(_.trim)
    SensorStatistic(sensorId = cols(0), humidity = cols(0))
  }
  val sink = Sink.foreach[SensorStatistic](println)

  val graph = source
    .via(flow)
    .to(sink)


  /**
   * Run the process
   */
  graph.run()
}
