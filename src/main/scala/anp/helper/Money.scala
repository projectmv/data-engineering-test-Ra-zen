package anp.helper

import java.util.{Currency, Locale}
import org.apache.spark.sql.functions.udf


object Money {
    //val to_money_udf = udf[Double, String, String](toMoney)
    val to_money_udf = udf((value: Double, locate: String) 
        => toMoney(value: Double, locate: String))
    def toMoney(value: Double, locate: String): String = {
        val cur = Currency.getInstance(new Locale(locate, locate.toUpperCase))
        val formatter = java.text.NumberFormat.getIntegerInstance
        formatter.setCurrency(cur)
        formatter.format(value)
    }
}
