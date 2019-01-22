import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()
  
  def distance(point:Point, point2:Point):Double={
        var d =Math.sqrt(scala.math.pow((point._1-point2._1),2)+ scala.math.pow((point._2-point2._2),2)) ;
        return d}
  
  def main(args: Array[ String ]) {

    val conf = new SparkConf().setAppName("KMSP")
    val sc = new SparkContext(conf)
    
    val points = sc.textFile(args(0)).map(line => { val p = line.split(",")
    (p(0).toDouble,p(1).toDouble) }).cache

    centroids = sc.textFile(args(1)).map( line => { val a = line.split(",")
                          (a(0).toDouble,a(1).toDouble) }).collect()
                          
    for ( i <- 1 to 5 ){
    	
      var cs = sc.broadcast(centroids)
      
    	var partition = points.map{ p => (cs.value.minBy(distance(p,_)),p) }
    			
    	var partitionmap = partition.mapValues(value=>(value,1))
    			
    	var keycountaggregation = partitionmap.reduceByKey{case(((x1,y1),c1),((x2,y2),c2))=> ((x1+ x2,y1+y2),(c1+c2))}
    	
    	var reduceset = keycountaggregation.mapValues{case(((xsum,ysum),count))=> (xsum/count,ysum/count)}
      
    	centroids = reduceset.map(_._2).collect()
    }
    
    centroids.foreach(println)
    
    sc.stop();
    
  }
}
