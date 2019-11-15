package org.hadoopexam.spark.core

object module6 {
  
  def main(args: Array[String]) : Unit = {
    
    val x = List.range(1, 10)
    
    x.foreach(println)
    
    var evens = x.filter((i:Int) => i%2 == 0).foreach(println)
    
    evens = x.filter(i => i%2 == 0).foreach((i:Int) => println)
    
    evens = x.filter(_ %2 == 2).foreach(println(_))
    
    
    val multiply = (i: Int) => (i*2)
    
    println(multiply(10))
    
    val list = List(1, 4)
    
   list.map(multiply).foreach(i => println(i))
    
   //Implicit approach 
   
   val addImp = (x:Int, y:Int) => x+y
   
   //explicit approach

   val addExp: (Int, Int) => Int = (x, y) => x+y 
   
   
   val helloWorld = () => {println("Hello World")} 
    
   acceptFunction(helloWorld)

   println("Executing function which print 3 times")
   executeNTimes(helloWorld, 3)
   
      val hello:(String) => String = (x) => { "Hello" + x }
  
   println(hello("varun"))
 
   acceptParamAndReturnFunction(hello, "Varun")
   
   val helloNoReturn:(String) => Unit = (x) => {println("Hello " +x)}
   
   acceptsFunctionWithParamaterAndReturnNothing(helloNoReturn, "Swathi")
 
   val add:(Int, Int) => Int = (x, y) => { x+y}
   
   val multiplyTemp = (x:Int, y:Int) => { x*y }
  
   acceptParamAndReturnParam(add, 10, 20 )
   
   acceptParamAndReturnParam(multiplyTemp, 1 , 3)
   
   
  }
  

  //Function which does not accept parameter and return nuthing
  def acceptFunction(f:() => Unit ) {
    f()
  }
  
  //function to execute N times 
  
  def executeNTimes(callback:() => Unit, x:Int) {
    for(i<-1 to x) {
      callback()
    }
  }
  
  
  //Functions which accepts String parameters and return nuthing
  def acceptsFunctionWithParamaterAndReturnNothing(f:(String)=> Unit , x:String ) {
     f(x)
    println("print result from function which accepts values and return nuthing ")
  }
  
  
  //Functions which accepts String parameters and return String paramater
  def acceptParamAndReturnFunction(f:(String) => String, x : String) {
      val result = f(x)
      println("print result from function which accepts values and return value "+result)  
  }
  
  
  //Function which accepst 2 Int parameter and return Int 
  
  def acceptParamAndReturnParam(callback:(Int, Int) => Int, x:Int, y:Int) : Unit = {
    
    val result = callback(x, y)
    println("executing param which accepts int and return and int value"+result)
    
  }
  
}