package tbench.tpcds.action

import org.apache.spark.sql.DataFrame
import org.junit.jupiter.api.Test
import tpcds.action.{QueryRun, QueryRunProfile, TPCDSQueryRunAction}

class TPCDSQueryActionTest {
    @Test
    def testRunProfile(): Unit = {
        val a1 = new TPCDSQueryTestRunAction
        a1.runProfileQueries("sf1-orc-run")
    }

}

class TPCDSQueryTestRunAction extends TPCDSQueryRunAction(null, "meta") {
    private var df: DataFrame = null

    def setReturnDF(df: DataFrame) = {
        this.df = df
    }

    override def getQueryRun(profile: String): Map[String, QueryRun] = {
        Map.empty
    }

    override def runTPCDSQuery(query: String): Unit = {
        // println(query)
        println(s"some query run ${query.take(5)}")
    }

    override def createQueryRun(entry: QueryRun): Unit = {
        println(s"create $entry")
    }

    override def getProfile(qn: String): Option[QueryRunProfile] = {
       Some(QueryRunProfile(
           "sf1-orc-run",
           "tpcds_sf1_orc",
         "tpcds",
           0,
           1,
           "{}",
           ""
       ))
    }

    override def setSparkConf(k: String, v: String): Unit = {
        println(s"set $k=$v")
    }

    override def runDDL(ql: String): Unit = {
        println(ql)
    }

    override def runSQL(ql: String): DataFrame = {
        println(ql)
        df
    }
}


