package calcscripts

import cn.com.xiaoxiang.common.tools.combinekeys.TextTuple
import org.apache.hadoop.io.{LongWritable, Text}
import shadoop.MapReduceTaskChain._
import shadoop._


/**
 * Created by zhouxiaoxiang on 15/1/12.
 */
object CampPV extends ScalaHadoop {

  import tools.TextComparableKeyElem._
  import shadoop.ImplicitConversion._

  val hashMapper = new Mapper[LongWritable, Text, TextTuple, LongWritable] {
    mapWith { (k, v) =>
      val content = v.split(",", -1)
      List((
        content(22) :: content(0),
        new LongWritable(1)))
    }
  }

  val hashReducer = new Reducer[TextTuple, LongWritable, TextTuple, LongWritable] {
    reduceWith { (k, v) =>
      List((
        k,
        new LongWritable(v.foldLeft[Long](0) {(acc, elem) => acc + elem.get()})
        ))
    }
  }

  val countMapper = new Mapper[TextTuple, LongWritable, Text, LongWritable] {
    mapWith { (k, v) =>
      List((
        new Text(k.get(0)),
        v
        ))
    }
  }

  val countReducer = new Reducer[Text, LongWritable, Text, LongWritable] {
    reduceWith {
      (k, v) =>
        List((
          k,
          new LongWritable(v.foldLeft[Long](0) {(acc, elem) => acc + elem.get()})
          ))
    }
  }

  def run(args: Array[String]): Int = {
    LzoTextInput[Text, LongWritable](args(0)) -->
    MapReduceTask(hashMapper, hashReducer, hashReducer, "shadoop camp pv example: hashing") -->
    NumReduceTasks(10) -->
    MapReduceTask(countMapper, countReducer, countReducer, "shadoop camp pv example: counting") -->
    NumReduceTasks(10) -->
    TextOutput[Text, LongWritable](args(1)) execute

    0
  }
}
