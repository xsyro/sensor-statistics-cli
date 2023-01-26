package com.jamiu.sensorstats

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}
import akka.stream.{ClosedShape, OverflowStrategy}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait DataProcessor {

  implicit val actorSystem = ActorSystem("sensor-statistics")

  def aggregate(records: Future[Array[Vector[String]]])(implicit ex: ExecutionContext) = {

    val sensorStatistics: Source[SensorStatistic, NotUsed] = Source.future(records.map(_.flatten.toVector)).flatMapConcat(iteration => Source.fromIterator(() => iteration.reverseIterator))
      .map { s =>
        val cols = s.split(",").map(_.trim)
        SensorStatistic(sensorId = cols(0), humidity = cols(1))
      }

    val input: Source[SensorStatistic, NotUsed] = sensorStatistics.fold(Set[String]())((acc, item) => acc + item.sensorId)
      .flatMapConcat(sets => Source.fromIterator(() => sets.iterator))
      .flatMapConcat((sensorId: String) => sensorStatistics.filter(f => f.sensorId.equals(sensorId)))

    val flow: Flow[SensorStatistic, SensorStatistic, NotUsed] = Flow[SensorStatistic]
      .buffer(50, OverflowStrategy.backpressure)

    val output = Sink.fold[List[SensorStatistic], SensorStatistic](List[SensorStatistic]()) { (acc, item) =>
      //persist the result to to memory location
      acc.::(item)
    }


    //After-all processes is done.
    val future = input.via(flow).log("some").runWith(output)

    future.map { computedLists =>
      println()
      println(s"Num of processed files: ${computedLists.count(_.sensorId equalsIgnoreCase "sensor-id")}")
      println(s"Num of processed measurements: ${computedLists.count(!_.humidity.equalsIgnoreCase("nan"))}")
      println(s"Num of failed measurements: ${computedLists.count(_.humidity.equalsIgnoreCase("nan"))}")

      println("\nSensors with highest avg list: \n")
      println("sensor-id,min,avg,max")
      val cleanRecords = computedLists
        .filterNot(_.humidity.equalsIgnoreCase("list"))
        .filterNot(_.sensorId.equalsIgnoreCase("sensor-id"))

      //StdOut Valid sensor-id
      cleanRecords.map(_.sensorId)
        .distinct //Set[String] for each sensorId
        .foreach { sensorId =>
        val list = cleanRecords
          .filter(_.sensorId equals sensorId)
          .filterNot(_.humidity equals "NaN")
          .filterNot(_.humidity.isEmpty)
          .map(_.humidity)
          .filter(_.matches("[0-9]+")) //filter to ensure only number are viable before mapTo[Int]
          .map(_.toInt)
        if (list.nonEmpty) println(s"$sensorId,${list.min},${list.sum / list.length},${list.max}")
      }

      //StdOut NaN sensor-id
      cleanRecords.filter(_.humidity.equals("NaN")).map(_.sensorId).distinct.foreach { item =>
        println(s"$item,NaN,NaN,NaN")
      }


    }
  }


  sealed case class SensorStatistic(sensorId: String, humidity: String)

}


/*
 * PoC rough sheet
 */
//    input.via(flow)
//      .runForeach(println)
//val headerSource: Source[String, NotUsed] = Source.combine(
//  Source.future(records.map(_.length).map(count => s"Num of processed files: $count")),
//  sensorStatistics.filterNot(_.list.equals("NaN")).fold(0)((x, _) => x + x).map(x => s"Num of processed measurements: $x"),
//  sensorStatistics.filter(_.list.equals("NaN")).fold(0)((x, _) => x + x).map(x => s"Num of failed measurements: $x"),
//  )(Merge(_))

//    RunnableGraph.fromGraph {
//      GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
//        import GraphDSL.Implicits._
//
//
//        val input = builder.add(sensorStatistics.fold(Set[String]())((acc, item) => acc + item.sensorId).flatMapConcat(iteration => Source.fromIterator(() => iteration.iterator))
//          .flatMapConcat(sensorId => sensorStatistics.filter(f => f.sensorId.equals(sensorId))))
//
//        val minHumidity = builder.add(Flow[SensorStatistic].map(x => x.list))
//        val avgHumidity = builder.add(Flow[SensorStatistic].map(x => x.list))
//        val maxHumidity = builder.add(Flow[SensorStatistic].map(x => x.list))
//        val nanHumidity = builder.add(Flow[SensorStatistic].filter(_.list equals "NaN").map(x => x.list))
//
//        val output = builder.add(Sink.foreach[SensorStatistic](println))
//
//        val broadcast = builder.add(Broadcast(4))
//        val zip = builder.add(Zip[SensorStatistic, SensorStatistic])
//
//        input ~> output
//        broadcast.out(0) ~> minHumidity ~> zip.in0
//        broadcast.out(1) ~> avgHumidity ~> zip.in1
////        broadcast.out(2) ~> maxHumidity ~> zip.in2
////        broadcast.out(3) ~> nanHumidity ~> zip.in2
//
//        zip.out ~> output
//
//
//
//        //        val input = builder.add(sensorStatistics)
//        //        val minHumidity = builder.add(Flow[SensorStatistic].merge())
//

//
//
//        //        builder.add(headerDetail.to(Sink.foreach[String](println)))
//
//
//        ClosedShape
//      }
//    }.run()