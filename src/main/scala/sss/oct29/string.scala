package sss.oct29

object string {
  def main(args: Array[String]) {
    val name1:String="Shyam kumar"
    val name2:String="Shyam"
    println(name1.indexOf('S'))
    println(name1.length)
    val str:Array[String]=name1.split(' ')
    str.foreach(x=>println(x))
    println(name1.charAt(0))
    println(name1.toLowerCase)
    println(name2.concat(" kumar") )
    println(name1 )
    println(name1.equals(name2) )

  }
}
