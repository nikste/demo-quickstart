package tryouts

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.api.scala._

import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream}
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.json.simple.parser.JSONParser
import org.json.simple.JSONObject
import org.json.simple.JSONArray
import org.apache.flink.contrib.streaming.scala.DataStreamUtils._
/**
  * Created by nikste on 03.12.15.
  */
object CoMapJsonJob {





  def main(args: Array[String]) {
    // set up the execution environment
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
          if (coordinates != null) {
            val coordinates1: JSONArray = coordinates.get("coordinates").asInstanceOf[JSONArray]

            // TODO: sometin's messed up yo!
            val lng: Double = coordinates1.get(0).asInstanceOf[Double]
            val lat: Double = coordinates1.get(1).asInstanceOf[Double]

            println("collecting:" + lat + " and " + lng)
            println(neLat + "," + neLng + "; " + swLat + "," + swLng)
            // if its inside of boundingbox:
            if (lat < neLat && lat > swLat && lng < neLng && lng > swLng) {
              //collector.collect(resString + "\t" + lat + "\t" + lng + "\n")
            }
          }
        }
      }
    }

//    new CoFlatMapFunction[JSONObject, String, String] {
//      val parser: JSONParser = new JSONParser
//
//      // downsample parameters
//      var passProbability: Double = 1.0
//
//      // filter by attribute
//      var neLat: Double = 0.0
//      var neLng: Double = 0.0
//      var swLat: Double = 0.0
//      var swLng: Double = 0.0
//
//      override def flatMap2(in: String, collector: Collector[String]): Unit = {
//        // data object
//
//        //TODO: include downsample
//
//        val elem = parser.parse(in).asInstanceOf[JSONObject]
//        var resString = ""
//
//        var entities = elem.get("entities").asInstanceOf[JSONObject]
//        var hashtags = entities.get("hashtags").asInstanceOf[JSONArray]
//
//        if (hashtags.size() > 0) {
//          val hashtag: JSONObject = hashtags.get(0).asInstanceOf[JSONObject]
//          val hashtagtext: String = hashtag.get("text").asInstanceOf[String]
//          resString = hashtagtext
//        } else {
//          resString = "hello"
//        }
//
//        //fill location statistics
//        val coordinates = elem.get("coordinates").asInstanceOf[JSONObject]
//        if (coordinates != null) {
//          val coordinates1: JSONArray = coordinates.get("coordinates").asInstanceOf[JSONArray]
//          val lat: Double = coordinates1.get(0).asInstanceOf[Double]
//          val lng: Double = coordinates1.get(1).asInstanceOf[Double]
//
//          // if its inside of boundingbox:
//          if (lat < neLat && lat > swLat && lng < neLng && lng > neLat) {
//            collector.collect(resString + "\t" + lng + "\t" + lat + "\n")
//          }
//        }
//      }
//
//      override def flatMap1(elem: JSONObject, collector: Collector[String]): Unit = {
//        // control objects
//        // convert to json
//        //          val elem = parser.parse(in).asInstanceOf[JSONObject]
//        //
//        val bounds = elem.get("bounds").asInstanceOf[JSONObject]
//
//        val northEast = bounds.get("_northEast").asInstanceOf[JSONObject]
//        val southWest = bounds.get("_southWest").asInstanceOf[JSONObject]
//
//        neLat = northEast.get("lat").asInstanceOf[Double]
//        neLng = northEast.get("lng").asInstanceOf[Double]
//
//        swLat = southWest.get("lat").asInstanceOf[Double]
//        swLng = southWest.get("lng").asInstanceOf[Double]
//
//        var items = elem.get("num_items").asInstanceOf[Integer]
//      }
//    }


    // extract relevant entities from tweets:
//    val tablerows = data.flatMap {
//      new FlatMapFunction[String, String] {
//        override def flatMap(elemString: String, collector: Collector[String]): Unit = {
//
//
//
//
//
//
//        }
//      }
//    }
    var it = collect(tablerows)

    tablerows.print()
    env.execute
  }
}
