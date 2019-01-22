import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  val depth = 6
  var count = 0

  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("GraphSc")

    val sc = new SparkContext(conf)

    def readgraph(row:Array[String]): (Long,Long,List[Long])={
      var clusterid = -1.toLong

      if(count<10){
        clusterid = row(0).toLong
        count = count+1
      }
      (row(0).toLong,clusterid,row.tail.map(_.toString.toLong).toList)
    }

    var gr = sc.textFile(args(0)).map(line => { readgraph(line.split(","))  })

    def eitherfunc(x:(Long, Long, List[Long])): List[(Long, Either[(Long,List[Long]),Long])] ={

      var templist = List[(Long, Either[(Long,List[Long]),Long])]()
        if(x._2>0) { x._3.foreach(adj=> {templist = (adj,Right(x._2))::templist} ) }
      templist = ( x._1 ,Left( x._2,x._3))::templist

      return  templist}

    def reduceFunc(tuple: (Long, Iterable[Either[(Long, List[Long]), Long]])): (Long,Long,List[Long])={
      var adjacent = List[Long]()
      var cluster = -1.toLong
      var vtxid = tuple._1

      for(aws<- tuple._2)
        aws match {
        case Right(c) => {cluster = c}
        case Left((c,adj)) if (c>0) =>  return (vtxid,c,adj)
        case Left((-1,adj)) => {adjacent = adj}
      }
      return (vtxid, cluster, adjacent)
    }

     for(i<-1 to depth){

       var intermediate = gr.flatMap( line=> { eitherfunc(line) } )
       var gpkey = intermediate.groupByKey;

       gr = gpkey.map(tup => reduceFunc(tup))
    }

    var agg = gr.map({ rr => (rr._2,1) })
    var finalcount = agg.reduceByKey(_+_)
    finalcount.collect().foreach(println)

    sc.stop();
  }
}
