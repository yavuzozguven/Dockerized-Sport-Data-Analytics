package com.yavuzozguven.GraphX

import java.awt.geom.Path2D
import java.awt.{Color, Image}
import java.io.File

import javax.imageio.ImageIO

import scala.swing.{Color, Graphics2D, Panel}
import scala.util.Random

class DataPanel(data: List[(Double,((String,(Int,Int)),Long))],network: List[((Double,Double),Long)]) extends Panel {

  override def paintComponent(g: Graphics2D): Unit = {

    val image = ImageIO.read(new File("src/main/resources/bg.jpg"))
    g.drawImage(image,0,0,1200,800,null)

    g.setColor(Color.white)

    data.map{r=>
      val width = (r._2._2*0.2).toInt
      g.fillOval(r._2._1._2._1*10 - width,r._2._1._2._2*10-width,(r._2._2*0.4).toInt,(r._2._2*0.4).toInt)
      val str = r._2._1._1.split(" ")
      g.drawString(str(str.size-1),r._2._1._2._1*10+width,r._2._1._2._2*10)
    }


    network.map{r=>
      val x1 = data.filter(_._1 == r._1._1).map(_._2._1._2._1).head
      val y1 = data.filter(_._1 == r._1._1).map(_._2._1._2._2).head
      val x2 = data.filter(_._1 == r._1._2).map(_._2._1._2._1).head
      val y2 = data.filter(_._1 == r._1._2).map(_._2._1._2._2).head

      var color = (r._2*20).toInt
      if(color > 255)
        color = 255


      g.setColor(new Color(255,255,255,color))


      g.drawLine(x1*10,y1*10,x2*10,y2*10)
    }


  }
}
