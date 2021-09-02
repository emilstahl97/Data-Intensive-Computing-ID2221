import scala.collection.mutable.ArrayBuffer

object ScalaTut {
  def main(args: Array[String]) : Unit = {

      var stack = new myStack()

      stack.push(1)
      stack.push(2)
      stack.push(3)
      stack.push(4)

      println(stack.peek())
 }
}

 class myStack() {
     
    var array = ArrayBuffer[Int]()

    def push(element:Int) : Unit = {
      this.array += element
 }

    def pop() : Unit = {
        this.array.remove(this.array.length - 1)
    }


def peek() : Unit = {
  println(this.array.mkString(","))
}

}

