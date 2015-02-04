package shadoop.wappers


import java.util

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter
import shadoop.ImplicitConversion._
import shadoop.Logging

import scala.beans.BeanProperty


/**
 * Created by zhouxiaoxiang on 15/1/15.
 */

sealed trait ScanElem {}

case class ScanWrapper(tableName: Option[String],
                       family: Option[String],
                       qualifier: Option[String],
                       startRow: Option[String],
                       endRow: Option[String],
                       timeRange: Option[(Long, Long)],
                       filter: Option[Filter]) extends Logging {

  val defaultMaxCaching = 5000
  val defaultCaching = 500
  var ifCaching = false

  @BeanProperty val scan = new Scan()
  family.map {f => scan.addFamily(f)}
  qualifier.map {q => scan.addColumn(family.get, q)}
  startRow.map(scan.setStartRow(_))
  endRow.map(scan.setStopRow(_))
  timeRange.map {tr => scan.setTimeRange(tr._1, tr._2)}
  filter.map(scan.setFilter)
  tableName.map(scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _))

  def boxing: Scan = if (ifCaching) scan else {val cachingAdded = caching(defaultCaching); cachingAdded.boxing}

  def caching(num: Int): ScanWrapper = {
    if (num > defaultMaxCaching) {
      info(s"set caching: $num exceeds limit: $defaultMaxCaching. use limit")
      scan.setCaching(defaultMaxCaching)
    } else {
      scan.setCaching(num)
    }
    ifCaching = true
    this
  }
}

object ScanWrapper {
  def apply(tableName: String, family: String, qualifier: String, filter: Filter)=
    new ScanWrapper(Option(tableName), Option(family), Option(qualifier), None, None, None, Option(filter))

  implicit def boxing(tp: (String, String, String))= apply(tp._1, tp._2, tp._3, null)
  implicit def boxing(tp: (String, String, String, Filter))= apply(tp._1, tp._2, tp._3, tp._4)

  def apply(tableName: String, family: String, qualifier: String, startRow: String, endRow: String, filter: Filter) =
    new ScanWrapper(Option(tableName), Option(family), Option(qualifier), Option(startRow),
      Option(endRow), None, Option(filter))

  implicit def boxing(tp: (String, String, String, String, String)) = apply(tp._1, tp._2, tp._3, tp._4, tp._5, null)
  implicit def boxing(tp: (String, String, String, String, String, Filter)) = apply(tp._1, tp._2, tp._3, tp._4, tp._5, tp._6)

  def apply(tableName: String, family: String, qualifier: String, startRow: String, endRow: String, startTime: Long, endTime: Long, filter: Filter) =
    new ScanWrapper(Option(tableName), Option(family), Option(qualifier), Option(startRow),
      Option(endRow), Option((startTime, endTime)), Option(filter))

  implicit def boxing(tp: (String, String, String, String, String, Long, Long, Filter)) =
    apply(tp._1, tp._2, tp._3, tp._4, tp._5, tp._6, tp._7, tp._8)
  implicit def boxing(tp: (String, String, String, String, String, Long, Long)) =
    apply(tp._1, tp._2, tp._3, tp._4, tp._5, tp._6, tp._7, null)

}

sealed trait ScansNil extends ScanElem {
  def ++(scanWrapper: ScanWrapper) = Scans(scanWrapper.boxing, this)
  def ++(scan: Scan) = Scans(scan, this)
}

case object ScansNil extends ScansNil {}

case class Scans(head: Scan, tail: ScanElem) extends ScanElem {
  def ++(scanWrapper: ScanWrapper) = Scans(scanWrapper.boxing, this)
  def ++(scan: Scan) = Scans(scan, this)
}



object Scans {
  def apply() = ScansNil
  def toJavaList(scans: Scans): util.List[Scan] = {
    val res = new util.ArrayList[Scan]()
    var thisElem = scans
    var quitCode = false
    while (!thisElem.isInstanceOf[ScansNil] && !quitCode) {
      res.add(thisElem.head)
      if (thisElem.tail.isInstanceOf[ScansNil]) {
        quitCode = true
      } else {
        thisElem = thisElem.tail.asInstanceOf[Scans]
      }
    }
    res
  }

  implicit def scansBoxing(tp: (String, String, String)) = apply() ++ tp
  implicit def scansBoxing(tp: (String, String, String, String, String, Filter)) = apply() ++ tp
  implicit def scansBoxing(tp: (String, String, String, Filter)) = apply() ++ tp
  implicit def scansBoxing(tp: (String, String, String, String, String)) = apply() ++ tp
  implicit def scansBoxing(tp: (String, String, String, String, String, Long, Long)) = apply() ++ tp
  implicit def scansBoxing(tp: (String, String, String, String, String, Long, Long, Filter)) = apply() ++ tp
  implicit def scansBoxing(scanWrapper: ScanWrapper) = apply() ++ scanWrapper

  def main(array: Array[String]) = {
    import ScanWrapper._
    println(toJavaList((("abc", "123", "456") caching 15000) ++ ("bcd", "456", "art")))
  }
}

