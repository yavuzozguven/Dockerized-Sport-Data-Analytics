package GraphX

import java.awt.Dimension
import scala.swing.{MainFrame, SimpleSwingApplication}

object Draw extends SimpleSwingApplication {

  var data: List[(Double, ((String, (Int, Int)), Long))] = null

  var network: List[((Double, Double), Long)] = null


  def top = new MainFrame {
    contents = new DataPanel(data, network) {
      centerOnScreen()
      preferredSize = new Dimension(1200, 800)
      pack()
    }
  }
}
