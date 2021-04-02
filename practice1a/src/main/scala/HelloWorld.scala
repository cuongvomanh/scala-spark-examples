import scala.collection.mutable

object HelloWorld extends App {
  // Ex1
//  helloWorld()
  // Ex2
//  scalaCollection()
  // Ex3
//  scalaTuple()
  // Ex4
//  println(addInt(1,4))
  // Ex5
//  val array = Array(1,2,3,4,5)
  // Ex5.1
//  array.foreach(x => println(x))
  // Ex5.2
//  array.foreach(println)
  // Ex5.3
//  array.filter(x => x % 3 == 0).foreach(println)
  // Pr1
  // producs la tap M
  val products = Array(("S10+", "Samsung"), ("iPhone", "Apple"), ("Note 20 Ultra", "Samsung"), ("Macbook", "Apple"), ("Galaxy Buds+", "Samsung"), ("Galaxy Wear", "Samsung"))
//  var A:mutable.Map[Char,Int] = mutable.Map()
//  A('I') = 1
//  A('J') = 5
//  A('K') = 10
  // Pr1.1
  // r la record
  // Mi tap tap con cua tap M thao man K=i
  // products.groupBy(r => r.company).mapValues(Mi => Mi.map(r => r.product))
  products.groupBy(r => r._2).mapValues(Mi => Mi.map(r => r._1))
    .foreach(x => {print("Key: "); println(x._1) ;println("Value: "); x._2.foreach(println)})
  // Pr1.2
//  val company = "Samsung"
//  products.filter(x => x._2 == company).foreach(println)
  // Pr1.3
//  val company = "Samsung"
//  products.filter(x => x._2 == company).map(r=> r._1).filter(x => x.matches(".*Galaxy.*")).foreach(println)

  def helloWorld(): Unit = {
    println("Hello World")
  }
  def scalaCollection(): Unit ={
    val fruits = Array("Peach", "Orange", "Apple")
    for (i <- 0 until fruits.size){
      println(fruits(i))
    }
  }
  def scalaTuple(): Unit ={
    val tuple = ("Apple", 1)
    println(tuple._1)
    println(tuple._2)
  }
  def addInt(a: Int, b: Int): Int = {
    return a + b
  }
  def increaseOneF(): Unit ={
    val increaseOne = (x: Int) => x +1
    print(increaseOne(6))
  }
}
