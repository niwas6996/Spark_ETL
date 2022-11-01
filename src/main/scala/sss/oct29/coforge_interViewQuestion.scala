package sss.oct29

object coforge_interViewQuestion extends App {

  val thirdCharToUpper = (str: String) =>// anonymus function
    if (str.length < 3) str
    else
      str.substring(0, 2) + str.substring(2, 3).toUpperCase + str.substring(3, str.length)

// make third character of word to UpperCase
//  val sample_string = Seq("swamiye saranam ayyappa")
  val sample_string = Seq(scala.io.StdIn.readLine())

  val splitString = sample_string.flatMap(f => f.split(" "))
    .map(word => thirdCharToUpper(word)).mkString(" ")

  println(splitString)

//count of words in a string
  val countWords = sample_string.flatMap(_.split(" "))
  var count = 0
  for (i <- countWords) {
    count += 1
  }
  println("count of words : " + count)

}