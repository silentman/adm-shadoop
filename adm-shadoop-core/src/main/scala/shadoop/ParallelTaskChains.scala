package shadoop

import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib.jobcontrol.{JobControl, ControlledJob}
import shadoop.IO.Input

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zhouxiaoxiang on 15/1/17.
 */
class ParallelTaskChains[KOUT, VOUT] extends Logging {
  val taskChains: ArrayBuffer[MapReduceTaskChain[_, _, KOUT, VOUT]] = ArrayBuffer.empty[MapReduceTaskChain[_, _, KOUT, VOUT]]

  def go(): MapReduceTaskChain[None.type, None.type, KOUT, VOUT] = {
    val controlledJobs = ArrayBuffer.empty[ControlledJob]
    val description = "parrallel %d task chains at %d".format(taskChains.size, System.currentTimeMillis())
    taskChains.foreach { tc =>
      val job = tc.setupJob()
      val ctrlJob = new ControlledJob(tc.getConf)
      ctrlJob.setJob(job)
      controlledJobs += ctrlJob
    }
    val jobControl = new JobControl(description)
    controlledJobs.foreach(jobControl.addJob)
    new Thread(jobControl).start()
    while (!jobControl.allFinished()) Thread.sleep(10000)
    if (jobControl.getFailedJobList.size() > 0) throw new RuntimeException("%s failed".format(description))
    val nextStageInputs = ArrayBuffer.empty[Input[KOUT, VOUT]]
    taskChains.foreach { tc =>
      nextStageInputs += new Input[KOUT, VOUT](tc.output.dirName, tc.output.outFormatClass.asInstanceOf[Class[InputFormat[KOUT, VOUT]]])
    }
    MapReduceTaskChain.init --> nextStageInputs.toArray
  }

  def &(mrtc: MapReduceTaskChain[_, _, KOUT, VOUT]) = taskChains += mrtc
}

object ParallelTaskChains {
  implicit def mrtcToPtc[KOUT, VOUT](mrtc: MapReduceTaskChain[_, _, KOUT, VOUT]) = new ParallelTaskChains[KOUT, VOUT] & mrtc
}
