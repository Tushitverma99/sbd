object test{
  def main(args:Array[String]):Unit=
    {

      //val-immutable-----------------
      val a = 5.5;
      println(a);

      //var-mutable-------------------
      var a1 = 3;
      println(a1);
      a1 = 78;

      //if-else----------------------
      println(a1);
      if (a1 % 2 == 0){
        println(a1 + " This is even")
      }
      else{
        println(a1 + " This is odd")
      }


      //loops-------------------------
      for(i<-1 to 5){
        println(i)
      }

      for (i<- 0 to 10 by 2){
        println(i)
      }

      for (i<- 0 until 10 by 2){
        println(i)
      }

      //while-loop---------------------
      var i=0;
      while(i<10){
        println("This is iteration : " + i)
        i+=1;
      }

      //array
      var arr = Array(10,203,40,50,60,708,90)

      println("Elements of this array : ")
      arr.foreach(println)
      println("the length of this array " + arr.length)

//      for(i <- until (arr.length)){
//      println(arr(i))
//    }
      //Tuple--------------------------
      var tup = (1,2,3,true,"Hey", 3.2)
      println(tup._2)

      for(i<- tup.productIterator){
        println(i)
      }

      //List
      var mylist = List(1,2,3,2,4,2,5,43,3,2)

      mylist.foreach(println)

      for (i<-0 until  mylist.length){
        println(mylist(i))
      }

      //set--------------------------

      var myset = Set(12,22,22,43,544,655,665,3,3,3)
      println(myset)





      println("-----------------Program Ends Here-------------------")
    }
}