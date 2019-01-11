import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class Point implements WritableComparable<Point> {
	
    public double x;
    public double y;
    
    Point(){}
    
    Point(double x1, double y1){
    	x=x1;
    	y=y1;
    }
    
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}
	
	public int compareTo(Point o) {
			return (int) (x==o.x? y-o.y:x-o.x);
	}
	
	@Override
	public boolean equals(Object o) {
		
		if(this==o) {
			return true;
		}
		if(!(o instanceof Point)) {  
			return false;
		}
		if(o instanceof Point) {
			Point other = (Point) o;
			return (this.x==other.x && this.y==other.y);
		}
		return false;
	}
	
	 @Override
	public int hashCode() {
	        final int prime = 31;
	        int result = 1;
	        result = prime * result
	                + ((x == 0) ? 0 : Double.hashCode(x));
	        return result;
	    }

	@Override
	public String toString() {
		return x+","+ y;
	}
}

 class Avg implements Writable{
	public double sumX;
	public double sumY;
	public long count;
	 
	public Avg() {}
	
	public Avg(double sumx, double sumy, long count) {
		this.sumX = sumx;
		this.sumY = sumy;
		this.count = count;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(sumX);
		out.writeDouble(sumY);
		out.writeLong(count);
	}
	
	public void readFields(DataInput in) throws IOException {
		sumX = in.readDouble();
		sumY = in.readDouble();
		count = in.readLong();
	}
 }

public class KMeans {
    static Vector<Point> centroids = new Vector<Point>(100);
    static Hashtable<Point, Avg> hashTable;
    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {
    	
    	@Override
    	public void setup(Context context) throws IOException {
    		
    		hashTable = new Hashtable<Point, Avg>();
    		URI[] paths = context.getCacheFiles();
    		Configuration conf = context.getConfiguration();
    		FileSystem fs = FileSystem.get(conf);
    		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
    		
    		String text="";
    		while((text=reader.readLine())!=null) {
    			String [] arr = text.split(",");
    			centroids.add(new Point(Double.parseDouble(arr[0]),Double.parseDouble(arr[1])));
    		}
    		reader.close();
    	}
    	
    	@Override
    	public void cleanup(Context context) throws IOException, InterruptedException {
    		
    		Set<Point> sp= hashTable.keySet();
			Iterator<Point> it = sp.iterator();
    		while(it.hasNext()) {
    			Point centroid = it.next();
    			context.write(centroid, hashTable.get(centroid));
    		}
    	}
    	
    	@Override
    	public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
    		Scanner s = new Scanner(value.toString()).useDelimiter(",");
    		double x = s.nextDouble();
    		double y = s.nextDouble();
    		Point p = new Point(x, y);
    		Iterator<Point> it = centroids.iterator();
    		double euclideanDist = Double.MAX_VALUE;
    		Point newcentroid = null;
    		
    		while(it.hasNext()) {
    			Point centroid = it.next();
    			
    			double dy = centroid.y - p.y;
    			double dx = centroid.x - p.x;
    			
    			if(Math.sqrt(dy*dy+dx*dx)< euclideanDist ) {
    				euclideanDist = Math.sqrt(dy*dy+dx*dx);
    				newcentroid = new Point(centroid.x, centroid.y);
    			}
    		}
    		
    		if(hashTable.get(newcentroid)==null) {
    			hashTable.put(newcentroid, new Avg(x,y,1));
    		}else {
    			Avg avg = hashTable.get(newcentroid);
    			hashTable.replace(newcentroid, new Avg(avg.sumX+x, avg.sumY+y, avg.count+1));
    		}
    		s.close();
    	}
    }

    public static class AvgReducer extends Reducer<Point,Avg,Point,Object> {
    	
    	@Override
    	public void reduce(Point centroidkey, Iterable<Avg> partialSumSet, Context context) throws IOException, InterruptedException {
    		
    		double sumx=0;
    		double sumy=0;
    		long count=0;
    		
    		for (Avg av : partialSumSet) {
    			sumx+= av.sumX;
    			sumy+= av.sumY;
    			count+= av.count;
			}
    		
    		centroidkey.x = sumx/count;
    		centroidkey.y = sumy/count;
    		
    		context.write(centroidkey, null);
    	}
    }

    public static void main ( String[] args ) throws Exception {
    
	    Job job = Job.getInstance();
	    job.setJobName("KMeansJob");
	    job.setJarByClass(KMeans.class);
	    
	    job.setOutputKeyClass(Point.class);
	    job.setOutputValueClass(Point.class);
	    
	    job.setMapOutputKeyClass(Point.class);
	    job.setMapOutputValueClass(Avg.class);
	    
	    job.setMapperClass(AvgMapper.class);
	    job.setReducerClass(AvgReducer.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job,new Path(args[0]));
	    FileOutputFormat.setOutputPath(job,new Path(args[2]));

	    job.addCacheFile(new URI(args[1]));
	    job.waitForCompletion(true);
   }
}
