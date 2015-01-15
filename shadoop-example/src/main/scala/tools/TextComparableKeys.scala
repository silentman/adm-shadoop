package tools

import cn.com.xiaoxiang.common.tools.combinekeys.TextTuple

/**
 * Created by zhouxiaoxiang on 15/1/13.
 */
object :: {
  def apply(s2: String, s1: TextTuple):TextTuple = {
    s1.add(0, s2)
    s1
  }
}

class TextComparableKeyElem(val e: TextTuple) {
  def ::(s: String) = tools.::(s, e)

  def >(tt: TextTuple): Boolean = {
    e.compareTo(tt) > 0
  }

  def <(tt: TextTuple): Boolean = {
    e.compareTo(tt) < 0
  }

  def ==(tt: TextTuple): Boolean = {
    e.compareTo(tt) == 0
  }

  def this(elem: String) {
    this(new TextTuple())
    e.add(elem)
  }
}

object TextComparableKeyElem {
  def apply(tt: TextTuple) = new TextComparableKeyElem(tt)
  def apply(s: String) = new TextComparableKeyElem(s)
  implicit def textTuple2TextComparableKeyElem(tt: TextTuple): TextComparableKeyElem = new TextComparableKeyElem(tt)
  implicit def string2TextComparableKeyElem(s: String):TextComparableKeyElem = TextComparableKeyElem(s)
}

object TextComparableKeys {

  def main(args: Array[String]) = {
    import TextComparableKeyElem._
    val test = "1" :: "2" :: "3"
//    val c = new TextTuple("1", "2", "3")
//    println(c)
    println(test)
  }
}