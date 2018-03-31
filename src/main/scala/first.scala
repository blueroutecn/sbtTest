import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}

object WordCount {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

//    val m=List(List(1,2),List(3,4))
//    println(m.map(x=>x))
//    println(m)
//    val x=m.flatten
//    println(x)
//    println(m.flatMap(x =>x))

//    val m1=List(List(1,2),List(3,4))
//    val m2=List(List(1,2),List(3,4))
//    val unionx=m1.union(m2)//把两个数据集联合起来
//    println(unionx)
//    val mx1=List(1,2)
//    val mx2=List(3,4)
//    val unionxx=mx1.union(mx2)//把两个数据集联合起来
//    println(unionxx)




    val inputFile =  "file:///home/johnhill_cn/IdeaProjects/sbtTest/Wordcount.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
//    val wordCount = textFile.flatMap(line => line.split(" "))
    val wordCount = textFile.filter(_.contains("s")).map(_.split(" ")(1).toInt)
    val hightAge = wordCount.sortBy(x => x,false).first
//    val wordCount = textFile.map(_.split(" ")(1))
//    val totalAge = wordCount.map(age => Integer.parseInt(
//      String.valueOf(age))).reduce((a,b) => a+b)
//    val totalAge = wordCount.map(age => Integer.parseInt(age)).reduce(_+_)
    println(wordCount)
    println(hightAge)
//      .map(word => (word, 1)).reduceByKey((a, b) => a + b)
//    println("avg age is " + totalAge.toDouble / wordCount.count())
//    wordCount.foreach(println(_))

//    val data1=sc.parallelize(List(1,2,3))//并行化，因为笛卡尔积是操作在RDD上的，所以必须是RDD的数据。
//    val data2=sc.parallelize(List(4,5,6))
//    data1.cartesian(data2).foreach(println)

    def sum(x: Int, y: Int, z: Int) = x + y + z
    val result = sum _ //表明是一个部分应用函数，参数一个都没定
    val r = result(2,3,4)
    println(r)

    val s = sum(4,8,_: Int)//注意这里得用_通配符，用i,j等不可以
    val s1 = s(7)
    println(s1)
  }
}