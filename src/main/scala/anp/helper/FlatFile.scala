package anp.helper

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.functions.lit


object FlatFile {


    //    def configMap(spark: SparkSession, config_path: String): Map[Int, Map[String, Any]] = {
    //        try {
    ////            val config_path = configManager.getString(conf_key)
    ////            val layout_df = Convert.jsonToDF(spark, layoutPath)
    //            val config_df = FlatFile.loadJson(spark, config_path)
    //            //val columns = layout_df.select("collumn").collect.map(x => x(0).toString)
    //            Convert.dfToMapIndex(config_df)
    //        } catch {
    //            case e : Exception =>
    //                log.error(s"Error not found config_key => ${{conf_key}} in file *.conf" +e.getMessage)
    //                e.printStackTrace()
    //                sys.exit(134)
    //        }
    //    }

    // val df = spark.read
    //     .format("com.crealytics.spark.excel")
    //     .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
    //     .option("header", "true") // Required
    //     .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
    //     .option("inferSchema", "false") // Optional, default: false
    //     .option("addColorColumns", "true") // Optional, default: false
    //     .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    //     .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
    //     .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
    //     .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
    //     .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
    //     .load("Worktime.xlsx")

    //    def getConfigLayout(spark: SparkSession, conf_key: String): Map[Int, Map[String, Any]] = {
    //        try {
    //            val layoutPath = configManager.getString(s"${{conf_key}}.layout")
    //            val layout_df = Convert.jsonToDF(spark, layoutPath)
    //            //val columns = layout_df.select("collumn").collect.map(x => x(0).toString)
    //            val layout = Convert.dfToMapIndex(layout_df)
    //            layout
    //        } catch {
    //            case e : Exception =>
    //                log.error(s"Erro not found config_key in file layout: "+e.getMessage)
    //                e.printStackTrace()
    //                sys.exit(134)
    //        }
    //    }

    //    def dfToJson(spark: SparkSession, pathConfig: String, base: String = ""): DataFrame = {
    //        val fileContext = spark.sparkContext.wholeTextFiles(pathConfig.toString).values
    //        val configDF = spark.read.json(fileContext)
    //        if (base != "") {
    //            val configFilterDF = configDF.filter("name='" + base.toString + "'")
    //            configFilterDF
    //        } else {
    //            configDF
    //        }
    //    }

       def loadFile(spark: SparkSession, spreadsheet: String): DataFrame = {
           //val log: Logger = Logger.getLogger(ExcelUtils.getClass)
           //log.info(s"loading excel")
           val df = spark.sqlContext.read.format("com.crealytics.spark.excel")
               .option("location", spreadsheet)
               //.option("sheetName", "full")
               .option("useHeader", "true")
               .option("treatEmptyValuesAsNulls", "false")
               .option("inferSchema", "false")
               .option("addColorColumns", "false")
               .option("startColumn", 0)
               .option("endColumn", 2)
               .option("excerptSize", 10)
               .load()
           //df.createOrReplaceTempView("tempByAutomatizador")
           //spark.sql(Integrity.insertTabAuto)
           df
       }
       
       def loadXls(spark: SparkSession, path_file: String): DataFrame = {
           val df = spark.sqlContext.read.format("com.crealytics.spark.excel")
               .option("location", path_file)
               .option("sheetName", "Categorias")
               .option("useHeader", "true")
               .option("treatEmptyValuesAsNulls", "true")
               .option("inferSchema", "true")
               .option("addColorColumns", "false")
               .load()
           // .option("startColumn", 0)
           // .option("endColumn", 2)
           // .option("excerptSize", 10)
           // df.createOrReplaceTempView("tempByAutomatizador")
           // spark.sql(Integrity.insertTabAuto)
           df
       }

    def readExcel(spark: SparkSession, file: String): DataFrame = {
        val df = spark.sqlContext.read
        .format("com.crealytics.spark.excel")
        .option("location", file)
        .option("useHeader", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .option("addColorColumns", "False")
        .load()
        df
    }

    def getConfigJson(spark: SparkSession, pathConfig: String, base: String = ""): DataFrame = {
        // val lines = spark.sparkContext.textFile("")
        val fileContext = spark.sparkContext.wholeTextFiles(pathConfig.toString).values
        val configDF = spark.read.json(fileContext)
        if (base != "") {
        val configFilterDF = configDF.filter("name='" + base.toString + "'")
        configFilterDF
    } else {
        configDF
    }
    }

    def getConfig(configDF: DataFrame, key: String): String = {
        val config = configDF.select(key).first().mkString
        config.replace("\\073",";")
    }

    def getConfigCol(configDF: DataFrame, baseDF: DataFrame): Array[String] = {
        var columns = configDF.select("column").first().mkString.toUpperCase()
        val columnFind = columns.split("\\s")(0)
        if (!baseDF.columns.contains(columnFind)) {
            columns = columns.toLowerCase()
        }
        columns.split("\\s")
    }

    def pathToFile(spark: SparkSession, pathFile: String) {
        //val tempFile = pathFile + ".tmp"
        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val oldFile = fs.globStatus( new Path( pathFile + "/part*" ) )( 0 ).getPath().getName()
        fs.rename( new Path( pathFile + "/" + oldFile ), new Path( pathFile ) )
        //fs.delete( new Path( pathFile ) )
    }

    // def getFileBase(spark: SparkSession, pathFile: String = "", configDF: DataFrame) : DataFrame = {
    //     val separator = getConfig(configDF, "separator")
    //     val fileDF = spark.read.csv(pathFile)
    //     val schemaFile = fileDF.first()(0).toString()
    //     val fields = schemaFile.split(separator).map(fieldName => StructField(fieldName, StringType, nullable = true))
    //     val schema = StructType(fields)
    //     val dfWithSchema = spark.read.option("header", "true").option("delimiter", separator).schema(schema).csv(pathFile)
    //     // .repartition(300)
    //     dfWithSchema
    // }

    def getFileDict(spark: SparkSession, pathFile: String = "", configDF: DataFrame) : DataFrame = {
        val separator = "\u0001"
        //val separator = getConfig(configDF, "separator-dict")
        val dfWithSchema = spark.read.option("delimiter", separator).csv(pathFile) //.repartition(300)
        dfWithSchema
    }

    def dfToFile(spark: SparkSession, df: DataFrame, pathFile: String): Unit = { //, configDF: DataFrame)
        // val separator = getConfig(configDF, "separator")
        val separator = "|"
        val tsvWithHeaderOptions: Map[String, String] = Map(
        ("delimiter", separator.toString),
        ("header", "true"),
        ("compression", "None"))

        val tempFile = pathFile + ".tmp"
        // val numPartitions = getNumPartitions()
        df.coalesce(1).write
        .mode(SaveMode.Overwrite)
        .options(tsvWithHeaderOptions)
        .format("csv")
        .save(tempFile)

        val fileFinal = pathFile + ".csv"
        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val oldFile = fs.globStatus(new Path(tempFile+"/part*"))(0).getPath().getName()
        fs.rename(new Path(tempFile +"/"+ oldFile), new Path(fileFinal))
        fs.delete(new Path(tempFile))
        println("file save in => " + fileFinal)
    }

    def saveDF(bulk: DataFrame, table_name: String): Unit = {
        bulk.write.
        mode("overwrite").
        insertInto(table_name)
    }

    //    def deleteTypeFile(types: Array[String]) = {
    //        import sys.process._
    //        val files = "ls".!!
    //        types.map(typ => {
    //            files.split("\n").map(f => {
    //                if (f.split('.').last.contains(typ)) {
    //                    new File(f).delete()
    //                }
    //            })
    //        })
    //    }

    def deleteTypeFile(types: Array[String]) = {
        import sys.process._
        val files = "ls".!!
        types.map(typ => {
        files.split("\n").map(f => {
        if (f.split('.').last.contains(typ)) {
        new File(f).delete()
    }
    })
    })
    }

    def getNumPartitions(): Int = {
        // TODO: calc number partitions proccess spark
        val numPartitions = 300
        numPartitions
    }

    @volatile private var instance: Broadcast [Array[(scala.util.matching.Regex, String)]] = null
    def getHostCategDictionary(spark: SparkSession, pathHdfs: String): Broadcast [Array[(scala.util.matching.Regex, String)]] = {
        if (instance == null) {
        //synchronized {
        if (instance == null) {
        val hostcategRDD = spark.sparkContext.textFile(pathHdfs)
        val hostRegExs = hostcategRDD.map( arr => {
        val line = arr.split(",")
        val expressao = line( 0 )
        val valor = line( 1 )
        (expressao.r, valor)
    } )
        instance = spark.sparkContext.broadcast(hostRegExs.collect())
    }
    }
        instance
    }

    def getConfigJsonTest(spark: SparkSession, pathConfig: String, base: String = ""): DataFrame = {
        val fileContext = spark.sparkContext.wholeTextFiles(pathConfig.toString).values
        val configDF = spark.read.json(fileContext)
        if (base != "") {
        val configFilterDF = configDF.filter("name='" + base.toString + "'")
        configFilterDF
    } else {
        configDF
    }
    }

    def saveJson(spark: SparkSession, df: DataFrame, pathHdfs: String) : Unit = {
        val tempFile = pathHdfs + ".tmp"
        df.coalesce(1).write.json(tempFile)
        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        fs.delete(new Path(pathHdfs))
        val oldFile = fs.globStatus(new Path(tempFile+"/part*"))(0).getPath().getName()
        fs.rename(new Path(tempFile +"/"+ oldFile), new Path(pathHdfs))
        fs.delete(new Path(tempFile))
        println("file save in => " + pathHdfs)
    }
}