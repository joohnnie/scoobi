/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi
package impl
package exec

import org.apache.hadoop.mapreduce.{Reducer => HReducer}
import scala.collection.JavaConversions._

import rtt._

/** Hadoop Combiner class for an MSCR. */
class MscrCombiner[V2] extends HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue] {

  private type Combiners = Map[Int, TaggedCombiner[_]]
  private var combiners: Combiners = _
  private var tv: TaggedValue = _

  override def setup(context: HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue]#Context) = {
    combiners = DistCache.pullObject[Combiners](context.getConfiguration, "scoobi.combiners").getOrElse(Map())
    tv = context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]
  }

  override def reduce(key: TaggedKey,
                      values: java.lang.Iterable[TaggedValue],
                      context: HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue]#Context) = {

    val tag = key.tag

    if (combiners.contains(tag)) {
      /* Only perform combining if one is available for this tag. */
      val combiner = combiners(tag).asInstanceOf[TaggedCombiner[V2]]

      /* Convert java.util.Iterable[TaggedValue] to Iterable[V2]. */
      val untaggedValues = new Iterable[V2] { def iterator = values.iterator map (_.get(tag).asInstanceOf[V2]) }

      /* Do the combining. */
      val reduction = untaggedValues.reduce(combiner.combine)
      tv.set(tag, reduction)

      context.write(key, tv)
    } else {
      /* If no combiner for this tag, TK-TV passes through. */
      values.foreach { value => context.write(key, value) }
    }
  }

  override def cleanup(context: HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue]#Context) = {
  }
}
