package com.jd.bgn.item_img2txt

import scala.collection.JavaConverters._
import java.io.File
import java.net.URL
import net.sourceforge.tess4j._
import scala.util.matching.Regex._
import javax.imageio.ImageIO
import org.apache.spark.SparkFiles


class Ocr extends Serializable {
  def run(url: String): String = {
    try {
      val image = ImageIO.read(new URL(url))
      if (image != null && image.getHeight >= 30 && image.getWidth >= 30) {
        Loader.load
        val tess = new Tesseract()
        tess.setDatapath(SparkFiles.get("tess_traineddata"))
        tess.setLanguage("chi_sim")
        tess.setOcrEngineMode(3)
        tess.setPageSegMode(3)
        tess.doOCR(image).replaceAll(" ", "").replaceAll("(NULL|\\t|\\n)", " ")
      } else {
        null
      }
    } catch {
      case e => null
    }
  }
}
object Loader extends Serializable {
  lazy val load = {
    val ocr_package = SparkFiles.get("linux-x86-64")
    System.load(ocr_package + "/libjpeg.so.8")
    System.load(ocr_package + "/libpng14.so.14")
    System.load(ocr_package + "/libtiff.so.3")
    System.load(ocr_package + "/libstdc++.so.6")
    System.load(ocr_package + "/liblept.so.5")
    System.load(ocr_package + "/libtesseract.so")
  }
}
