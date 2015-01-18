package calcscripts

import java.util

import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import shadoop._
import shadoop.MapReduceTaskChain._
import shadoop.wappers.Scans._


/**
 * Created by zhouxiaoxiang on 15/1/15.
 */
object HBaseSiteScan extends ScalaHadoop {
  implicit def string2Bytes(s: String): Array[Byte] = Bytes.toBytes(s)

  def run(args: Array[String]): Int = {

    val limit = new PageFilter(1000)
    val mapper = new TableMapper[Text, Text] {
      mapWith {
        (k, v, ctx) =>
          List((
            new Text(Bytes.toString(k.get())),
            new Text(Bytes.toString(v.getValue("column_family", "admckid")))
            ))
      }
    }

    TableMapReduceTask(mapper, "shadoop hbase example",
      ("id_mapping_index", "column_family", "admckid", "1277:", "1277?", limit) ++
        ("id_mapping_index", "column_family", "admckid", "1100:", "1100?", limit)) -->
    Param("hbase.zookeeper.quorum", "hbaseJT") -->
    NumReduceTasks(0) -->
    TextOutput[Text, Text](args(0)) execute



    0
  }

}
