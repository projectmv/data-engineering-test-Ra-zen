package anp.bussiness.transform

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SegmentTransform extends CommonTransform {

    val segmentSchema = StructType(Array(
        StructField("ANO", IntegerType, nullable = true),
        StructField("SEGMENTO", StringType, nullable = true),
        StructField("ESTADO", StringType, nullable = true),
        StructField("Jan", DoubleType, nullable = true),
        StructField("Fev", DoubleType, nullable = true),
        StructField("Mar", DoubleType, nullable = true),
        StructField("Abr", DoubleType, nullable = true),
        StructField("Maio", DoubleType, nullable = true),
        StructField("Jun", DoubleType, nullable = true),
        StructField("Jul", DoubleType, nullable = true),
        StructField("Ago", DoubleType, nullable = true),
        StructField("Set", DoubleType, nullable = true),
        StructField("Out", DoubleType, nullable = true),
        StructField("Nov", DoubleType, nullable = true),
        StructField("Dez", DoubleType, nullable = true),
        StructField("Total", DoubleType, nullable = true)))


    def execute(spark: SparkSession): DataFrame = {
        log.info(s"Initial read xls segment... -> ")
        val worksheet = "segment"
        val sheetPos = "!A1:P5000"
        val ws = mergeWorkSheets(spark, worksheet, sheetPos, segmentSchema)
        println(s"worksheet: $worksheet quant-> " + ws.count)
        aggRegion(ws, "SÃO PAULO", "2017").show
        ws
    }



}

