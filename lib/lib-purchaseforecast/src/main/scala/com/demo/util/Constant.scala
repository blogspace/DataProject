package com.demo.util

object Constant {
  //sparkConf参数
  val appName = "dwo_user_app_opt"
  val master = "local[*]"
  //数据源
  val inpath = "D:\\workspace\\idea\\DataProject\\PurchaseForecast\\src\\data"
  val action = s"${inpath}\\jdata_action.csv"
  val comment = s"$inpath{inpath}\\jdata_comment.csv"
  val product = s"$inpath{inpath}\\jdata_product.csv"
  val shop =s"$inpath{inpath}\\jdata_shop.csv"
  val user=s"${inpath}\\jdata_user.csv"

}
