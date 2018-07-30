package com.jd.bgn.utils

import java.io._
import scala.collection.JavaConverters._
class AttrSetFileIO extends Serializable {
  def write(attr_set: Map[(String, String, String, String), Array[(String, String, Int, Array[String])]], path: String): Unit = {
    val fos = new FileOutputStream(path)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(attr_set)
    oos.close
  }
  def read(path: String): Map[(String, String, String, String), Array[(String, String, Int, Array[String])]] = {
    val fis = new FileInputStream(path)
    val ois = new ObjectInputStream(fis)
    val attr_set = ois.readObject.asInstanceOf[Map[(String, String, String, String), Array[(String, String, Int, Array[String])]]]
    ois.close
    attr_set
  }
}