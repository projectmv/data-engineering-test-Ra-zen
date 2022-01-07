package anp.bussiness.transform

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object FuelTransform extends CommonTransform {

    val fuelSchema = StructType(Array(
        StructField("COMBUSTÍVEL", StringType, nullable = true),
        StructField("ANO", IntegerType, nullable = true),
        StructField("REGIÃO", StringType, nullable = true),
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
        log.info(s"Initial read xls fuel... -> ")
        val worksheet = "fuel"
        val sheetPos = "!A1:Q5000"
        val ws = mergeWorkSheets(spark, worksheet, sheetPos, fuelSchema)
        println(s"worksheet: $worksheet quant-> " + ws.count)
        aggRegion(ws, "SÃO PAULO", "2017").show
        ws
    }

  


}

