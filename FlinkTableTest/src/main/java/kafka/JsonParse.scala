package kafka

/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName kafka
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/27 - 9:29
  */
import scala.util.parsing.json._
object JsonParse {
  def main(args: Array[String]): Unit = {
    def regJson(json:Option[Any])=json match {
      case Some(map:Map[String,Any]) => map
    }
      val str = "{\"host\":\"td_test\",\"ts\":1486979192345,\"device\":{\"tid\":\"a123456\",\"os\":\"android\",\"sdk\":\"1.0.3\"},\"time\":1501469230058}"
      val jsons = JSON.parseFull(str)
      val first = regJson(jsons)
    //获取一级key
    println(first.get("host"))
    //获取二级key
    val dev = first.get("device")
    println(dev)
    val sec = regJson(dev)
    println(sec.get("tid").toString.replace("Some(","").replace(")",""))
  }
}
