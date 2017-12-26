import $ivy.`co.fs2::fs2-core:0.10.0-M10`
import $ivy.`co.fs2::fs2-io:0.10.0-M10`
import $ivy.`com.nrinaudo::kantan.csv:0.3.0`
import $ivy.`com.nrinaudo::kantan.csv-generic:0.3.0`

import cats.effect.IO
import fs2.text
import java.nio.file.Paths
import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._

final case class Point(latitude: Double, longitude: Double)

implicit val pointCellDecoder: CellDecoder[Point] = {
  val regex = """Point\((.+) (.+)\)""".r
  CellDecoder.from { (s: String) =>
    DecodeResult {
      val regex(lat, lon) = s
      Point(lat.toDouble, lon.toDouble)
    }
  }
}

final case class City(
  item: String,
  label: String,
  point: Point,
  locode: Option[String]
)

val cities = fs2.io.file.readAll[IO](Paths.get("cities.csv"), 4096)
  .through(text.utf8Decode)
  .through(text.lines)
  .map(_.readCsvRow[City](rfc))
  .collect { case Success(city) => city }
  .map(println)
  .run

cities.unsafeRunSync
