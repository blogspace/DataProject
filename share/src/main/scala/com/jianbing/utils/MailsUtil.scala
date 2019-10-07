package com.jianbing.utils
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import play.api.libs.mailer._


/**
  * @function
  * @author create by liuhao at 2019/7/30 17:50
  */
object MailsUtil {
  /**
    * 定义一个发邮件的人
    * @param host STMP服务地址
    * @param port STMP服务端口号
    * @param user STMP服务用户邮箱
    * @param password STMP服务邮箱密码
    * @param timeout setSocketTomeout 默认: 60s
    * @param connectionTimeout setSocketConnectionTimeout 默认：60s
    * @return  返回一个可以发邮件的用户
    */
  def createMailer(host:String, port: Int, user: String, password: String, timeout:Int = 10000, connectionTimeout:Int = 10000):SMTPMailer ={
    // STMP服务SMTPConfiguration
    val configuration = new SMTPConfiguration(
      host, port, false, false, false,
      Option(user), Option(password), false, timeout = Option(timeout),
      connectionTimeout = Option(connectionTimeout), ConfigFactory.empty(), false
    )
    val mailer: SMTPMailer = new SMTPMailer(configuration)
    mailer
  }


  /**
    * 生成一封邮件
    * @param subject 邮件主题
    * @param from 邮件发送地址
    * @param to 邮件接收地址
    * @param bodyText 邮件内容
    * @param bodyHtml 邮件的超文本内容
    * @param charset 字符编码 默认： utf-8
    * @param attachments 邮件的附件
    * @return 一封邮件
    */
  def createEmail(subject:String, from:String, to:Seq[String], bodyText:String = "ok", bodyHtml:String = "", charset:String = "utf-8", attachments:Seq[Attachment] = Seq.empty): Email = {

    val email = Email(subject, from, to,
      bodyText = Option[String](bodyText), bodyHtml = Option[String](bodyHtml),
      charset= Option[String](charset),attachments = attachments

    )
    email

  }

  /**
    *  生成一个附件
    * @param name 附件的名字
    * @param fileStr 以本地文件为附件相关参数
    * @param rdd 以hdfs文件或rdd或df为附件相关参数
    * @return
    */
  def createAttachments(name: String, fileStr: String = "", rdd:RDD[String] = null): Attachment  = {
    var attachment: Attachment = null
    if(fileStr.contains(":")){
      val file: File = new File(fileStr)
      attachment = AttachmentFile(name, file)
    }else{
      val data: Array[Byte] = rdd.collect().mkString("\n").getBytes()
      // 根据文件类型选择MimeTypes对应的值
      val mimetype = "text/plain"
      attachment = AttachmentData(name, data, mimetype)
    }
    attachment
  }


  /**
    *  主要针对日常简单结果的快速发送
    * @param subject 邮件主题名字
    * @param toStr 邮件的接收人，多名以,分割
    * @param bodyText 邮件的内容
    * @return 用户设备 <510109769.1.1561635225728@RAN>
    */
  def dailyEmail(subject:String, toStr:String, bodyText: String):String={
    val to = toStr.split(",").toList
    // 阿里云企业 邮箱
    val host = "smtp.mxhichina.com"
    val port = 25
    val user = "@.com"
    val password = ""
    val from = user
    val mailer: SMTPMailer = MailsUtil.createMailer(host, port, user, password)
    val email: Email = MailsUtil.createEmail(subject, from, to, bodyText = bodyText)
    val userdev: String = mailer.send(email)
    userdev
  }


}
