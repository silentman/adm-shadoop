package shadoop

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io._

object ImplicitConversion {
  // Handle BooleanWritable
  implicit def BooleanWritableUnbox(v: BooleanWritable): Boolean = v.get

  implicit def BooleanWritableBox(v: Boolean): BooleanWritable = new BooleanWritable(v)

  // Handle DoubleWritable
  implicit def DoubleWritableUnbox(v: DoubleWritable): Double = v.get

  implicit def DoubleWritableBox(v: Double): DoubleWritable = new DoubleWritable(v)

  // Handle FloatWritable
  implicit def FloatWritableUnbox(v: FloatWritable): Float = v.get

  implicit def FloatWritableBox(v: Float): FloatWritable = new FloatWritable(v)

  // Handle IntWritable
  implicit def IntWritableUnbox(v: IntWritable): Int = v.get

  implicit def IntWritableBox(v: Int): IntWritable = new IntWritable(v)

  // Handle LongWritable
  implicit def LongWritableUnbox(v: LongWritable): Long = v.get

  implicit def LongWritableBox(v: Long): LongWritable = new LongWritable(v)

  // Handle Text
  implicit def TextUnbox(v: Text):String = v.toString

  implicit def TextBox(v: String): Text = new Text(v)

  implicit def StringBuilderBox(v: StringBuilder): Text = new Text(v.toString)

  implicit def StringBufferBox(v: StringBuffer): Text = new Text(v.toString)

  implicit def MapWritableBox[X <: Writable, Y <: Writable](value: scala.collection.Map[X, Y]): MapWritable = {
    var newMap = new MapWritable()
    value.foreach {
      case (k, v) => newMap.put(k, v)
    }
    return newMap
  }

  implicit def string2Bytes(s: String) = Bytes.toBytes(s)

  implicit def bytes2String(bs: Array[Byte]) = Bytes.toString(bs)
}
