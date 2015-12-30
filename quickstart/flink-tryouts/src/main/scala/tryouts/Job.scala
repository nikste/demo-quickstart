package tryouts

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.twitter.{TwitterFilterSource, TwitterFilterSourceExample, TwitterSource}
import org.apache.flink.util.Collector
import org.json.simple.{JSONArray, JSONObject}
import org.json.simple.parser.JSONParser

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   mvn clean package
 * }}}
 * in the projects root directory. You will find the jar in
 * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    // track these terms:
    //var termsArray:Array[String] = Array[String]("instacool","deutschland","obama","usa","fuck","yeah","dude","awesome","neuland")

    //for(i <- 0 until termsArray.length){
    var source = new TwitterFilterSource("/home/nikste/Desktop/twitterKeys2")
//    source.filterLanguage("de");
    source.trackTerm("obama");
    var streamSourceObama = env.addSource(source)
    //    source.filterLanguage("en");

    var streamSource = streamSourceObama
    var source2 = new TwitterFilterSource("/home/nikste/Desktop/twitterKeys2")
    source2.trackTerm("berlin")

    var streamSourceBerlin = env.addSource(source2)
//    var streamSource

    var stream = streamSourceObama.connect(streamSourceBerlin).map(new CoMapFunction[String,String,String] {
      override def map1(in1: String): String = {
        return in1
      }
      override def map2(in2: String): String = {
        return in2
      }
    })

    stream.print()
    //stream.print
    //val data: DataStream[String] = env.addSource(new RMQSource[String]("localhost", "DATA", new SimpleStringSchema)) //FLINK_DATA

    // extract relevant entities from tweets:
    /*val tablerows = data.flatMap {
      new FlatMapFunction[String, String] {
        override def flatMap(elemString: String, collector: Collector[String]): Unit = {

          val parser: JSONParser = new JSONParser

          val elem = parser.parse(elemString).asInstanceOf[JSONObject]


          //fill location statistics
          val coordinates = elem.get("coordinates").asInstanceOf[JSONObject]
          if (coordinates != null) {
            val coordinates1: JSONArray = coordinates.get("coordinates").asInstanceOf[JSONArray]
            val lat: Double = coordinates1.get(0).asInstanceOf[Double]
            val lon: Double = coordinates1.get(1).asInstanceOf[Double]

            collector.collect("" + lon + "\t"+ lat + "\n")
          }
        }
      }
    }*/


    // extract relevant entities from tweets:
    val tablerows = stream.flatMap {
      new FlatMapFunction[String, String] {
        override def flatMap(elemString: String, collector: Collector[String]): Unit = {
          //println("processing Tweet:" + elemString)
          val parser: JSONParser = new JSONParser

          val elem = parser.parse(elemString).asInstanceOf[JSONObject]

          var resString = ""

          var entities = elem.get("entities").asInstanceOf[JSONObject]
          if(entities != null){
            var hashtags = entities.get("hashtags").asInstanceOf[JSONArray]

            if(hashtags.size > 0){
              val hashtag: JSONObject = hashtags.get(0).asInstanceOf[JSONObject]
              val hashtagtext: String = hashtag.get("text").asInstanceOf[String]
              resString = hashtagtext
            }else{
              resString = "hello"
            }

            //fill location statistics
            val coordinates = elem.get("coordinates").asInstanceOf[JSONObject]
            if (coordinates != null) {
              val coordinates1: JSONArray = coordinates.get("coordinates").asInstanceOf[JSONArray]
              val lat: Double = coordinates1.get(0).asInstanceOf[Double]
              val lng: Double = coordinates1.get(1).asInstanceOf[Double]

              collector.collect(resString + "\t" + lng + "\t"+ lat + "\n")
            }//else{
            //              collector.collect(resString + "\t" + 11 + "\t" + 52 + "\n")
            //            }
          }
        }
      }
    }
    tablerows.print()

    failAfter(Span(100, Millis)) {
      Thread.sleep(200)
    }
    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}
