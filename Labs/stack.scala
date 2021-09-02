import scala.collection.mutable.ArrayBuffer

object ScalaTut {
  def main(args: Array[String]) : Unit = {

     var stack = new myStack() // create a new stack

      def callStack() : Unit = {
      stack.push(1)
      stack.push(2)
      stack.push(3)
      stack.push(4)

      stack.peek()

      stack.pop()
      
      stack.peek()

      println("Is element 4 in the stack?  " + stack.search(4))

      println("Is stack empty? " + stack.isEmpty())
      }

      callStack()
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

    def search(element:Int) : Boolean = {
      this.array.contains(element)
    }

    // check if array is empty    
    def isEmpty() : Boolean = {
      this.array.isEmpty
    }
  }






