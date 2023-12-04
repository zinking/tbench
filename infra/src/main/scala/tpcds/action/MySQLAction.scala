package tpcds.action

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import java.util.Properties
import scala.io.Source

class MySQLAction(configFile: String) {
  val driver = "com.mysql.jdbc.Driver"

  def getConfig(configFile: String): MySQLConfig = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val content = Source.fromResource(configFile).mkString
    val confMap = parse(content).extract[Map[String, String]]
    MySQLConfig(
      confMap.getOrElse("host", ""),
      confMap.getOrElse("port", ""),
      confMap.getOrElse("user", ""),
      confMap.getOrElse("pass", ""),
      confMap.getOrElse("db", ""),
      confMap.getOrElse("tb", "")
    )
  }
  def saveDataFrame(df: DataFrame): Unit = {
    val config: MySQLConfig = getConfig(configFile)
    config.validate()
    val url = s"jdbc:mysql://${config.host}:${config.port}/${config.db}"
    val prop = new Properties()
    prop.put("url", url)
    prop.put("user", config.user)
    prop.put("password", config.pass)
    prop.put("driver", driver)

    df.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, config.tb, prop)
  }

}

case class MySQLConfig(host: String, port: String, user: String, pass: String, db: String, tb: String) {
  def validate(): Unit = {
    assert(StringUtils.isNotEmpty(host), "host required")
    assert(StringUtils.isNotEmpty(port), "port required")
    assert(StringUtils.isNotEmpty(user), "user required")
    assert(StringUtils.isNotEmpty(pass), "pass required")
    assert(StringUtils.isNotEmpty(db), "db required")
    assert(StringUtils.isNotEmpty(tb), "tb required")
  }

}
