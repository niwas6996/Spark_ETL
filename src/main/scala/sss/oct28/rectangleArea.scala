package sss.oct28

object rectangleArea {
  def main(args: Array[String]): Unit = {


  class Rectangle( l: Int,  b: Int) {
    val length: Int = l
    val breadth: Int = b
    def getArea: Int = l * b
    override def toString = s"This is rectangle with length as $length and breadth as  $breadth"
  }
      val rect = new Rectangle(4, 5)
      println(rect.toString)
      println(rect.getArea)
    }
  }



