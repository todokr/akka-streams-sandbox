import java.nio.file.Paths
import akka.util.ByteString
import akka.stream.scaladsl.JsonFraming
import akka.stream.scaladsl.Compression
import akka.stream.scaladsl.Flow
import java.nio.file.StandardOpenOption._

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import spray.json.{ DefaultJsonProtocol, _ }
import purecsv.safe._

object StreamingCopy extends DefaultJsonProtocol {

  implicit val rowFormat = jsonFormat2(Row)
  case class Row(id: Int, score: Double)
  case class Floored(id: Int, score: Int)

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val ec = system.dispatcher
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val in = Paths.get("./sample.json.gz")
    val out = Paths.get("./output.txt")

    val source = FileIO.fromPath(in)
    val sink = FileIO.toPath(out, Set(CREATE, WRITE, APPEND))

    val framingJson = Compression.gunzip().via(JsonFraming.objectScanner(Integer.MAX_VALUE)).map(_.utf8String)
    val parsing = Flow[String].map(line => line.parseJson.convertTo[Row])
    val floor = Flow[Row].map(r => Floored(r.id, Math.floor(r.score).toInt))
    val toCsv = Flow[Floored].map(_.toCSV() + "\n")

    val blueprint = source.via(framingJson).via(parsing).via(floor).via(toCsv).map(ByteString.fromString).to(sink)

    blueprint.run()

    Thread.sleep(1000 * 10) // TODO sleep?

    system.terminate()
  }
}
