package com.jd.bgn.item_img2txt

import scala.collection.JavaConverters._
import scala.util.matching.Regex._
import org.jsoup.Jsoup

class WdisParser() extends Serializable {
  def parse(wdis: String): Array[String] = {
    if (wdis == null) {
      Array[String]()
    } else {
      val parser = Jsoup.parse(wdis)
      val elements = parser.select("img[src]")
      elements.eachAttr("src")
              .asScala
              .toArray
              .map((url) => {
                val url1 = url.trim
                val url2 = if (!url1.startsWith("http://") && !url1.startsWith("https://")) {
                  "http:" + url1.replace("360buyimg.com","360buyimg.local")
                } else {
                  url1.replace("360buyimg.com","360buyimg.local")
                }
    	          "http.*\\.(jpg|png)".r.findFirstMatchIn(url2) match {
                  case Some(url3) => url3.toString
                  case None => null
                }
    	        })
    	        .filter(_ != null)
    }
  }
}