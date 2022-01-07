package anp.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


object SparkUtils {
    def getSparkSession(fonte: String, queue: String="default", qt_exec_master: String = "*"): SparkSession = {
        SparkSession
            .builder()
            .appName("decode-"+ fonte)
            .config("spark.master", "local")
            //.config("spark.master", "yarn")
            .config("spark.yarn.queue", queue)
            .getOrCreate()
    }

}