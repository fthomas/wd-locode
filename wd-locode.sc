import $ivy.`co.fs2::fs2-core:0.10.0-M10`
import $ivy.`co.fs2::fs2-io:0.10.0-M10`
import $ivy.`com.nrinaudo::kantan.csv:0.3.0`
import $ivy.`com.nrinaudo::kantan.csv-generic:0.3.0`

import cats.effect.IO
import java.nio.file.Path
import java.nio.file.Paths
import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._

def readAllLines(path: Path): fs2.Stream[IO, String] =
  fs2.io.file.readAll[IO](path, 8192)
    .through(fs2.text.utf8Decode)
    .through(fs2.text.lines)

///

final case class Point(latitude: Double, longitude: Double)

implicit val pointCellDecoder: CellDecoder[Point] = {
  val regex = """Point\((.+) (.+)\)""".r
  CellDecoder.from { (s: String) =>
    DecodeResult {
      val regex(lon, lat) = s
      Point(lat.toDouble, lon.toDouble)
    }
  }
}

final case class City(
  item: String,
  name: String,
  point: Point,
  locode: Option[String]
)

val citiesStream = readAllLines(Paths.get("cities.csv"))
  .map(_.readCsvRow[City](rfc))
  .collect { case Success(city) => city }
  .runLog

val cities: Vector[City] = citiesStream.unsafeRunSync

///

// http://www.unece.org/cefact/codesfortrade/codes_index.html

final case class Locode(
  changeInd: String,
  code: String,
  name: String,
  state: String,
  point: Option[Point]
)

final case class Locode2(
  code: String,
  name: String,
  point: Point
)

implicit val locodeRowDecder: RowDecoder[Locode] =
  RowDecoder.ordered { (
     changeInd: String,
     country: String,
     city: String,
     name: String,
     _: String,
     state: String,
     _: String,
     _: String,
     _: String,
     _: String,
     coords: String
    ) =>
      val regex = """(\d{2})(\d{2})N (\d{3})(\d{2})E""".r
      val point = coords match {
        case regex(latDeg, latMin, lonDeg, lonMin) =>
          Some(Point(
            latDeg.toDouble + (latMin.toDouble / 60.0),
            lonDeg.toDouble + (lonMin.toDouble / 60.0)))
        case _ => None
      }

      Locode(changeInd, country + city, name, state, point)
  }

val locodesStream = readAllLines(Paths.get("locodes.csv"))
  .map(_.readCsvRow[Locode](rfc))
  .collect { case Success(locode) => locode }
  .collect { case Locode(_, code, name, _, Some(point)) => Locode2(code, name, point) }
  .runLog

val locodes: Vector[Locode2] = locodesStream.unsafeRunSync()

// assume the earth is flat :-)
def dist(p1: Point, p2: Point): Double =
  math.sqrt(
    math.pow(p1.latitude  - p2.latitude,  2) +
    math.pow(p1.longitude - p2.longitude, 2)
  )

def findLocode(city: City): Option[Locode2] = {
  locodes.find { locode =>
    locode.name == city.name && {
      val d = dist(locode.point, city.point)
      d < 0.03
    }
  }
}

val citiesWithLocodes: Vector[(City, Locode2)] =
  cities.flatMap(city => findLocode(city).map(locode => (city, locode)))
