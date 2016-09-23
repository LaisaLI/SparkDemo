import java.io.{File, PrintWriter}
import java.text.{ParsePosition, SimpleDateFormat}
import java.util.Date

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**

  * 用于重庆市两客一危GPS数据的预处理
  * step1：以车辆ID为key，只提取车辆ID，时间，经度和纬度信息
  * step2：每个key按照时间进行排序
  * step3：结果保存到同一个分区中
  */


object WordCount {

  val dirIn = "/UserData/重庆数据/2016年两客一危GPS/两客一危2016.3月/0301.txt"
//  val dirOut = "/test_count_out_chongqing_0301_BP2968_4.txt"
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val line = sc.textFile(dirIn)
  def main(args: Array[String]) {


    val result = line.map(x=>{
      val xArr = x.split(";")
      (xArr(0).substring(2,8),xArr(0).substring(2,8)+","+xArr(3)+","+xArr(19)+","+xArr(20))
    })
    //表示将以车牌作为key的RDD，进行GroupByKey操作生成的RDD[String,Iterable[String]],再flatMap（转换为List，按照时间排序）
    val result1 = result.groupByKey().flatMap((x)=>x._2.toList.sortWith((x1,x2)=>sortWithTime(x1,x2)))


    //将结果保存到同一个分区中
    result1.repartition(1).saveAsTextFile("/UserData/重庆数据/0301result2.txt")
    sc.stop()

  }

  //filterOfOneCar用于将某辆车的数据筛选出来,
  //dataCleaning用于将这辆车的数据中的ID，时间，经度和纬度提取出来。
  def filtOneCar(): Unit ={

    val filterOfOneCar = line.filter(x=> x.split(";")(0).substring(2,8).equals("H70125"))
    val dataCleaning = filterOfOneCar.map(x=>{
      val xArr = x.split(";")
      xArr(0).substring(2,8)+","+xArr(3)+","+xArr(19)+","+xArr(20)
    })

    filterOfOneCar.repartition(1).saveAsTextFile("/UserData/重庆数据/H70125.txt")
    dataCleaning.repartition(1).saveAsTextFile("/UserData/重庆数据/H70125Cleaned.txt")
  }

  //为sortWith函数提供排序算子
  def sortWithTime(str1:String,str2:String):Boolean={
    val time1 = strToDate(strTrans(str1.split(",")(1)))
    val time2 = strToDate(strTrans(str2.split(",")(1)))
    if(time1!=null && time2!=null){
      if( time1.before(time2)){
        true
      }
      else{false}
    }
    else{println( "time1 or time2 is null")
      true}
  }
  //针对重庆的时间数据做的字符串截取原格式：2016/03/01 08:38:52.000000000 转换后的格式："yyyyMMdd HHmmss"
  def strTrans(timeStr:String):String={
    val time =timeStr.substring(0,4)+timeStr.substring(5,7)+timeStr.substring(8,10)+" "+timeStr.substring(11,13)+timeStr.substring(14,16)+timeStr.substring(17,19)
    return time
  }

  //利用java中的时间格式转换函数，将data格式为“yyyyMMdd HHmmss”string转换为Date类型
  def strToDate(timeStr:String):Date={
    val formatter = new SimpleDateFormat("yyyyMMdd HHmmss")
    val pos = new ParsePosition(0);
    val strtodate = formatter.parse(timeStr, pos);
    return strtodate;
  }

}