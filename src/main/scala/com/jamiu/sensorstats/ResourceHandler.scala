package com.jamiu.sensorstats

import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, ExecutionContext, Future}

/*
 * CSVFileHandler follow flow of how sensor data can be aggregated.
 */
trait ResourceHandler  {

  def filesInDirectory: Array[File]

  def contentOfEachFile: Array[Vector[String]]

  def filter(pred: File => Boolean): ResourceHandler = {
    Good(filesInDirectory.filter(pred), contentOfEachFile)
  }

  def readFile[A](func: File => Vector[String]): Array[Vector[String]] = {
    val arr = filesInDirectory.map(func)
    Good(pathToDirectory = filesInDirectory, contentOfFileInDirectory = arr)
    filesInDirectory.map(func)
  }

  def readFileAsync[A](func: File => Future[Vector[String]])(implicit executionContext: ExecutionContext): Future[Array[Vector[String]]] = {
    val futureSeq = filesInDirectory.map(f => func(f))
    Future.sequence(futureSeq.toSeq).map(_.toArray)
  }

  def printDirectory = {
    println("\n≈≈≈≈ Quick view of the files in the specified path ≈≈≈≈")
    filesInDirectory.foreach(f => println(f.getAbsolutePath))
    this
  }

}

final case class Good(pathToDirectory: Array[File], contentOfFileInDirectory: Array[Vector[String]] = Array.empty) extends ResourceHandler {
  override def filesInDirectory = pathToDirectory
  override def contentOfEachFile = contentOfFileInDirectory
}

final case class Bad() extends ResourceHandler {
  override def filesInDirectory = Array.empty
  override def contentOfEachFile = Array.empty
}


object ResourceHandler {

  import scala.language.implicitConversions
  implicit def optionDirToArrayOfFilesInDir(dir: String): Array[File] = if (!new File(dir).isDirectory) Array.empty else new File(dir).listFiles()

  def apply[A](pathToDirectory: => Option[String]): ResourceHandler = {
    pathToDirectory match {
      case Some(dir) => Good(dir)
      case None => Bad()
    }
  }

}
