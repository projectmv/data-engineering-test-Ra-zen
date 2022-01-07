package anp

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import anp.helper.SparkUtils
import anp.bussiness.Anp


object Execute {
    val log: Logger = Logger.getLogger(Execute.getClass)
    log.info(s"*************************************  test1  *********************************************")

    val configManager = ConfigFactory.load()
    log.info(s"*************************************  test2  *********************************************")
    def main(args: Array[String]): Unit = {

        val loglevel = configManager.getString(s"log.level")
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        //Logger.getLogger("hadoop").setLevel(Level.OFF)
        loglevel match {
            case "INFO" => log.setLevel(Level.INFO)
            case "DEBUG" => log.setLevel(Level.DEBUG)
            case "WARN" => log.setLevel(Level.WARN)
            case "ERROR" => log.setLevel(Level.ERROR)
            case _ => log.setLevel(Level.INFO)
        }
        log.info(s"[*] Initializing process - Log Level: $loglevel")
        val fonte = args(0)
        val worksheet = args(1)
        log.info(s"Initializing process")
        if (fonte.isEmpty || args.contains("-h"))
            showHelp()
        val spark = SparkUtils.getSparkSession(fonte)

        log.info(s"**********************************************************************************")
        log.info(s"*** Fonte: $fonte")
        log.info(s"*** Application ID: " + spark.sparkContext.applicationId)
        log.info(s"**********************************************************************************")

        try {
            fonte match {
                case "anp" =>
                    log.info(s"call => $fonte... ")
                    Anp.fuel(spark, worksheet)
                    Anp.segment(spark, worksheet)

                case _ =>
                    log.error(fonte + s" Fonte not found!")
                    sys.exit(134)
            }
        } catch {
            case e: Exception =>
                log.error(s"Erro no processo: " + e.getMessage)
                e.printStackTrace()
                sys.exit(134)
        }
        log.info(s"Final proccess")
    }

    def showHelp(): Unit = {
        log.info(s" ==> HELP")
        log.info(s"======================================================================================== ")
        log.info("usage    : spark-submit consolidate-spark-assembly-0.1.jar --fonte $fonte")
        log.info("$fonte   : font name, like (consolidate, etc...)")
        log.info("generics : usar fonte_name para executar somente uma das fontes")
        log.info(s"========================================================================================")
        sys.exit(134)
    }
}
