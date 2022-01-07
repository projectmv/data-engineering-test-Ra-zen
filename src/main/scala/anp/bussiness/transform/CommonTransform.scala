package anp.bussiness.transform

import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import anp.helper._


trait CommonTransform {
    val log: Logger = Logger.getLogger(this.getClass)
    val configManager = ConfigFactory.load()
    val file = configManager.getString(s"sheet.path_hdfs")

    def aggRegion(df: DataFrame, uf: String, ano: String): DataFrame = {
        val region = df.filter(col("ESTADO")===lit(uf) && col("ANO") === lit(ano))
            .agg(
                sum(col("Jan")).as("jan"), 
                sum(col("Fev")).as("fev"),
                sum(col("Mar")).as("mar"),
                sum(col("Abr")).as("abr"),
                sum(col("Maio")).as("mai"),
                sum(col("Jun")).as("jun"),
                sum(col("Jul")).as("Jul"),
                sum(col("Ago")).as("ago"),
                sum(col("Set")).as("set"),
                sum(col("Out")).as("out"),
                sum(col("Nov")).as("nov"),
                sum(col("Dez")).as("dez")
            )
            .withColumn("ano", lit(ano)) 
            .withColumn("jan", Money.to_money_udf(col("jan"), lit("br")))
            .withColumn("fev", Money.to_money_udf(col("fev"), lit("br")))
            .withColumn("mar", Money.to_money_udf(col("mar"), lit("br")))
            .withColumn("abr", Money.to_money_udf(col("abr"), lit("br")))
            .withColumn("mai", Money.to_money_udf(col("mai"), lit("br")))
            .withColumn("jun", Money.to_money_udf(col("jun"), lit("br")))
            .withColumn("jul", Money.to_money_udf(col("jul"), lit("br")))
            .withColumn("ago", Money.to_money_udf(col("ago"), lit("br")))
            .withColumn("set", Money.to_money_udf(col("set"), lit("br")))
            .withColumn("out", Money.to_money_udf(col("out"), lit("br")))
            .withColumn("nov", Money.to_money_udf(col("nov"), lit("br")))
            .withColumn("dez", Money.to_money_udf(col("dez"), lit("br")))
        region
    }

    def mergeWorkSheets(spark: SparkSession, worksheet: String, sheetPos: String, schema: StructType): DataFrame = {
        var sheets = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        for (i <- 1 to 3) {
            val position = decimalToBinary(i.toInt)
            val ws = configManager.getConfig(s"sheet.$worksheet.worksheets.$position").root.get("ws").render  
            sheets = readWorkSheet(spark, ws, sheetPos, schema).unionAll(sheets)
        }
        sheets
    }

    def readWorkSheet(spark: SparkSession, ws: String, sheetPos: String, schema: StructType): DataFrame = {       
        val dataAddress = "'" + ws.substring(1, ws.length -1) + "'" +  sheetPos
        val file = configManager.getString(s"sheet.path_hdfs")
        // var schema = getSchema(ws)
        readXls(spark, file, dataAddress, schema)
    }

    // def getSchema(ws: ConfigFactory): StructType = {
    //     var schema: Array()
    //     for (i <- 1 to 17) {
    //         val position = decimalToBinary(i)
    //         col = ws.getConfig(position).root.get("collumn")
    //         typ = ws.getConfig(position).root.get("type")
    //         schema.append(s"StructField($col, $typ, nullable = false)")
    //     }
    //     StructType(schema)
    // }

    def readXls(spark: SparkSession, file: String, dataAddress: String, schema: StructType): DataFrame = {
        spark.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("dataAddress", dataAddress)
            .option("inferSchema", true)
            .schema(schema)
            .load(file)
    }

	def decimalToBinary(num: Int, leng: Int=5): String = {
		var flag: Int = 0;
     	var number: Int = num;
        var res =""
		if (number < 0)
		{
			number = -number;
		}
		var bits: Int = 31;
		while (bits >= 0)
		{
			if (((number >> bits) & 1) == 1)
			{
                res += "1"
				flag = 1;
			}
			else if (flag == 1)
			{
                res += "0"
			}
			bits -= 1;
		}
        s"%0${leng}d".format(res.toInt).toString
	}
}
