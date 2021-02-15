package com.yavuzozguven.GraphX
import swing.{MainFrame, Panel, SimpleSwingApplication}
import java.awt.{Color, Dimension, Graphics2D}


object Draw extends SimpleSwingApplication {

  var data : List[(Double,((String,(Int,Int)),Long))] = null

  var network : List[((Double,Double),Long)] = null


  def top = new MainFrame {
    contents = new DataPanel(data,network) {
      centerOnScreen()
      preferredSize = new Dimension(1200, 800)
      pack()
    }
  }
}