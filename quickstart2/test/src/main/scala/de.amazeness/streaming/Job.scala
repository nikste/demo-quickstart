package de.amazeness.streaming

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

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.scala.DataStreamUtils._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

import org.apache.flink.contrib.streaming.scala.DataStreamUtils
import org.apache.flink.contrib.streaming.java.DataStreamIterator
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.json.simple.{JSONArray, JSONObject}
import org.json.simple.parser.JSONParser

/**
  * Skeleton for a Flink Job.
  *
  * For a full example of a Flink Job, see the WordCountJob.scala file in the
  * same de.amazeness/directory or have a look at the website.
  *
  * You can also generate a .jar file that you can submit on your Flink
  * cluster. Just type
  * {{{
  *   mvn clean de.amazeness
  * }}}
  * in the projects root directory. You will find the jar in
  * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
  *
  */

object Job {
  def main(args: Array[String]) {


    val env = StreamExecutionEnvironment.getExecutionEnvironment


    var data: DataStream[String] = env.addSource(new RMQSource[String]("localhost", "DATA", new SimpleStringSchema)) //FLINK_DATA

    val feedback_in: DataStream[String] = env.addSource(new RMQSource[String]("localhost", "mikeQueue", new SimpleStringSchema))

    val feedback: DataStream[JSONObject] = feedback_in.map{
      new MapFunction[String,JSONObject]{
        override def map(t: String): JSONObject = {
          println(t)
          val parser: JSONParser = new JSONParser
          val elem = parser.parse(t).asInstanceOf[JSONObject]
          return(elem)
        }
      }
    }
    // downsample data:

    var dataConnected: ConnectedStreams[String, JSONObject] = data.connect(feedback);
    val tablerows: DataStream[String] = dataConnected.flatMap {
      new CoFlatMapFunction[String, JSONObject, String] {
        def convertToDouble(in: String) : Double = {
          val neD = in contains "."
          neD match {
            case false => return( (in + ".0").asInstanceOf[Double] )
            case _ => return( in.asInstanceOf[Double])
          }
        }
        // downsample parameters
        var passProbability: Double = 1.0

        // filter by attribute
        var neLat: Double = 999999
        var neLng: Double = 99999
        var swLat: Double = -99999
        var swLng: Double = -999999

        override def flatMap2(elem: JSONObject, collector: Collector[String]): Unit = {

          val control = elem.get("control").asInstanceOf[JSONObject]

          val bounds = control.get("bounds").asInstanceOf[JSONObject]

          val northEast = bounds.get("_northEast").asInstanceOf[JSONObject]
          val southWest = bounds.get("_southWest").asInstanceOf[JSONObject]

          neLat = southWest.get("lat").asInstanceOf[Number].doubleValue()
          neLng = southWest.get("lng").asInstanceOf[Number].doubleValue()

          swLat = southWest.get("lat").asInstanceOf[Number].doubleValue()
          swLng = southWest.get("lng").asInstanceOf[Number].doubleValue()


          var items = elem.get("num_items").asInstanceOf[Integer]

          println("got control message!")
          println("nelat:"+ neLat + "neLng:" + neLng + "swLat:" + swLat + "swLng:" + swLng)
        }

        override def flatMap1(in: String, collector: Collector[String]): Unit = {

          val parser: JSONParser = new JSONParser

          val elem = parser.parse(in).asInstanceOf[JSONObject]
          var resString = ""

          var entities = elem.get("entities").asInstanceOf[JSONObject]
          var hashtags = entities.get("hashtags").asInstanceOf[JSONArray]

          if (hashtags.size() > 0) {
            val hashtag: JSONObject = hashtags.get(0).asInstanceOf[JSONObject]
            val hashtagtext: String = hashtag.get("text").asInstanceOf[String]
            resString = hashtagtext
          } else {
            resString = "hello"
          }

          //fill location statistics
          val coordinates = elem.get("coordinates").asInstanceOf[JSONObject]
          //println("got tweet!")
          if (coordinates != null) {
            val coordinates1: JSONArray = coordinates.get("coordinates").asInstanceOf[JSONArray]



            // TODO: sometin's messed up yo!
            val lng: Double = coordinates1.get(0).asInstanceOf[Double]
            val lat: Double = coordinates1.get(1).asInstanceOf[Double]

            //println("collecting:" + lat + " and " + lng)
            //println(neLat + "," + neLng + "; " + swLat + "," + swLng)
            // if its inside of boundingbox:
            if (lat < neLat && lat > swLat && lng < neLng && lng > swLng) {
              println("inside box")
              collector.collect(resString + "\t" + lat + "\t" + lng + "\n")
            }/*else{
                //println("outside box")
                collector.collect(resString + "\t" + lat + "\t" + lng + "\n")
            }*/
          }
        }
      }
    }



    var it: DataStreamIterator[String] = collect(tablerows).asInstanceOf[DataStreamIterator[String]]


   for(i <- 0 until 1000){
      for(j <- 0 until 3){
        var n = it.hasImmidiateNext(1)
        println("hasnext? :" + n)
        if(n){
          var next = it.next()
          println("nextval:" + next)
        }
      }
    }



    /**
      * Here, you can start creating your execution plan for Flink.
      *
      * Start with getting some data from the environment, like
      * env.readTextFile(textPath);
      *
      * then, transform the resulting DataSet[String] using operations
      * like:
      *   .filter()
      *   .flatMap()
      *   .join()
      *   .group()
      *
      * and many more.
      * Have a look at the programming guide:
      *
      * http://flink.apache.org/docs/latest/programming_guide.html
      *
      * and the examples
      *
      * http://flink.apache.org/docs/latest/examples.html
      *
      */


    // execute program
    //env.execute("Flink Scala API Skeleton")
  }
}
