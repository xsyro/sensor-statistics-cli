package com.jamiu.sensorstats.main

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ClosedShape, OverflowStrategy}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import com.jamiu.sensorstats.{DataProcessor, ResourceHandler}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.io.{Source => SourceIO}
import scala.language.implicitConversions

object Application extends App with DataProcessor {


  implicit val fileReaderExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  val csvRecords = ResourceHandler(args.headOption.orElse {
    println("Enter absolute path containing all the CSV files")
    SourceIO.stdin.getLines().nextOption()
  })
    .filter(file => file.isFile && !file.isHidden)
    .filter(file => file.getName.substring(file.getName.length - 4).equals(".csv"))
    .printDirectory
    .readFileAsync { javaIoFile =>
      Future {
        val bfSource = SourceIO.fromFile(javaIoFile)
        val lines = bfSource.getLines().toVector
        bfSource.close()
        lines
      }
    }

  /**
   * Backed by Akka Stream API!
   */
  aggregate(csvRecords)

}
