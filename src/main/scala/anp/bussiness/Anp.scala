package anp.bussiness

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import anp.bussiness.transform.{FuelTransform, SegmentTransform}
import com.crealytics.spark.excel._
import io.delta.tables._


object Anp {
    val log: Logger = Logger.getLogger(Anp.getClass)

    def fuel(spark: SparkSession, worksheet: String): Unit = {
        try {
            log.info(s"Initial read apn xls.. -> ")
            // val fuelTransform = new FuelTransform
            FuelTransform.execute(spark)
        } catch {
            case e: Exception => log.info(e.printStackTrace())
        }
    }

    def segment(spark: SparkSession, worksheet: String): Unit = {
        try {
            log.info(s"Initial read apn xls.. -> ")
            SegmentTransform.execute(spark)
        } catch {
            case e: Exception => log.info(e.printStackTrace())
        }
    }

    def getStorage(spark: SparkSession) = {
        DeltaTable.createOrReplace(spark)
            .tableName("test.anp_info")
            .addColumn("year_month", "STRING")
            .addColumn("uf", "STRING")
            .addColumn("product", "STRING")
            .addColumn("unit", "STRING")
            .addColumn("volume", "double")
            .addColumn("created_at", "TIMESTAMP")
            .partitionedBy("year_month")
            .execute()
    }

}
