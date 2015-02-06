/**
 * Copyright (C) 2013 Adam Retter (adam.retter@googlemail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package shadoop


import org.apache.hadoop.conf._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapper => HTableMapper, AdmasterTableMapReduceUtil, TableMapReduceUtil}
import org.apache.hadoop.io.{Text, LongWritable, Writable, WritableComparable}
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.{Mapper => HMapper}
import org.apache.hadoop.mapreduce.{Reducer => HReducer}
import org.apache.hadoop.mapreduce.Job
import shadoop.wappers.Scans

import scala.collection.mutable.ArrayBuffer


abstract class MR[KIN, VIN, KOUT, VOUT](val mapper: Option[MP[KIN, VIN, _, _]],
                                    val combiner: Option[Reducer[_, _, _, _]],
                                    val reducer: Option[Reducer[_, _, KOUT, VOUT]],
                                    val name: String) {
  def initJob(conf: Configuration): Job
}

case class MapReduceTask[KIN, VIN, KOUT, VOUT](override val mapper: Option[MP[KIN, VIN, _, _]],
                                               override val combiner: Option[Reducer[_, _, _, _]],
                                               override val reducer: Option[Reducer[_, _, KOUT, VOUT]],
                                               override val name: String)
  extends MR[KIN, VIN, KOUT, VOUT](mapper, combiner, reducer, name) {

  override def initJob(conf: Configuration): Job = {
    val job = new Job(conf, this.name)

    mapper match {
      case Some(m) =>
        job.setMapperClass(m.getClass.asInstanceOf[Class[HMapper[KIN, VIN, _, _]]])
        job.setJarByClass(m.getClass)

      case None =>
    }

    combiner match {
      case Some(c) =>
        job.setCombinerClass(c.getClass.asInstanceOf[Class[HReducer[_, _, _, _]]])
      case None =>
    }

    reducer match {
      case Some(r) =>
        job.setReducerClass(r.getClass.asInstanceOf[Class[HReducer[_, _, KOUT, VOUT]]])
        job.setOutputKeyClass(r.kType)
        job.setOutputValueClass(r.vType)

        mapper match {
          case Some(m) =>
            job.setMapOutputKeyClass(m.kType)
            job.setMapOutputValueClass(m.vType)
          case None =>
            job.setMapOutputKeyClass(r.kinType)
            job.setMapOutputValueClass(r.vinType)
            job.setJarByClass(r.getClass)
        }

      case None if (!mapper.isEmpty) =>
        job.setOutputKeyClass(mapper.get.kType)
        job.setOutputValueClass(mapper.get.vType)
    }

    job
  }
}


object MapReduceTask {

  def apply[KIN, VIN, KOUT, VOUT](
                                   mapper: Mapper[KIN, VIN, KOUT, VOUT],
                                   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = apply(Option(mapper), None, None, name)

  def apply[KIN, VIN, KOUT, VOUT](
                                   reducer: Reducer[KIN, VIN, KOUT, VOUT],
                                   name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] = apply[KIN, VIN, KOUT, VOUT](None, None, Option(reducer), name)

  def apply[KIN, VIN, KOUT, VOUT, KTMP, VTMP](
                                               mapper: Mapper[KIN, VIN, KTMP, VTMP],
                                               reducer: Reducer[KTMP, VTMP, KOUT, VOUT],
                                               name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] =
    apply[KIN, VIN, KOUT, VOUT](Option(mapper), None, Option(reducer), name)

  def apply[KIN, VIN, KOUT, VOUT, KTMP, VTMP](
                                               mapper: Mapper[KIN, VIN, KTMP, VTMP],
                                               combiner: Reducer[_, _, _, _],
                                               reducer: Reducer[KTMP, VTMP, KOUT, VOUT],
                                               name: String): MapReduceTask[KIN, VIN, KOUT, VOUT] =
    apply[KIN, VIN, KOUT, VOUT](Option(mapper), Option(combiner), Option(reducer), name)
}


case class TableMapReduceTask[KOUT, VOUT](override val mapper: Option[TableMapper[_, _]],
                                          override val combiner: Option[Reducer[_, _, _, _]],
                                          override val reducer: Option[Reducer[_, _, KOUT, VOUT]],
                                          override val name: String)
  extends MR[ImmutableBytesWritable, Result, KOUT, VOUT](mapper, combiner, reducer, name) {

  var scans:Scans = null

  def this(mapper: Option[TableMapper[_, _]],
           combiner: Option[Reducer[_, _, _, _]],
           reducer: Option[Reducer[_, _, KOUT, VOUT]],
           name: String, scans: Scans) = {
    this(mapper, combiner, reducer, name)
    this.scans = scans
  }
  override def initJob(conf: Configuration): Job = {
    val job = new Job(conf, name)

    mapper match {
      case Some(m) =>
        job.setJarByClass(m.getClass)
        AdmasterTableMapReduceUtil.initTableMapperJob(Scans.toJavaList(scans),
          m.getClass.asInstanceOf[Class[HTableMapper[_, _]]],
          m.kType,
          m.vType,
          job)
      case None =>
        throw new IllegalStateException("hbase job mapper should not be emtpy")
    }

    reducer match {
      case Some(r) =>
        job.setReducerClass(r.getClass.asInstanceOf[Class[Reducer[_, _, KOUT, VOUT]]])
        job.setOutputKeyClass(r.kType)
        job.setOutputValueClass(r.vType)
      case None =>
        if (!mapper.isEmpty) {
          job.setOutputKeyClass(mapper.get.kType)
          job.setOutputValueClass(mapper.get.vType)
        }

    }

    job
  }
}

object TableMapReduceTask {
  def apply[KOUT, VOUT](mapper: TableMapper[KOUT, VOUT], name: String, scans: Scans) =
    new TableMapReduceTask[KOUT, VOUT](Option(mapper), None, None, name, scans)

  def apply[KOUT, VOUT](mapper: TableMapper[_, _], reducer: Reducer[_, _, KOUT, VOUT], name: String, scans: Scans) = {
    new TableMapReduceTask[KOUT, VOUT](Option(mapper), None, Option(reducer), name, scans)
  }

  def apply[KOUT, VOUT](mapper: TableMapper[_, _],
                        combiner:  Reducer[_, _, _, _],
                        reducer: Reducer[_, _, KOUT, VOUT],
                        name: String,
                        scans: Scans) =
    new TableMapReduceTask[KOUT, VOUT](Option(mapper), Option(reducer), Option(reducer), name, scans)
}