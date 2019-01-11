import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
	public String toString() {
		return x+","+ y;
	}
	
}

public class KMeans {
    static Vector<Point> centroids = new Vector<Point>(100);

    public static class AvgMapper extends Mapper<Object,Text,Point,Point> {
    	
    	@Override
    	public void setup(Context context) throws IOException {
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
    	public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
    		Scanner s = new Scanner(value.toString()).useDelimiter(",");
    		double x = s.nextDouble();
    		double y = s.nextDouble();
    		Point p = new Point(x, y);
    		Iterator<Point> it = centroids.iterator();
    		double euclideanDist = Double.MAX_VALUE;
    		Point newcentroid = new Point();
    		
    		while(it.hasNext()) {
    			Point centroid = it.next();
    			
    			double dy = centroid.y - p.y;
    			double dx = centroid.x - p.x;
    			
    			if(Math.sqrt(dy*dy+dx*dx)< euclideanDist ) {
    				euclideanDist = Math.sqrt(dy*dy+dx*dx);
    				newcentroid.x = centroid.x;
    				newcentroid.y = centroid.y;
    			}
    		}

    		context.write(newcentroid, p);
    	}
    }

    public static class AvgReducer extends Reducer<Point,Point,Point,Object> {
    	
    	@Override
    	public void reduce(Point centroidkey, Iterable<Point> newpointset, Context context) throws IOException, InterruptedException {
    		
    		double newx=0;
    		double newy=0;
    		int count=0;
    		
    		for (Point point : newpointset) {
    			count++;
    			newx+=point.x;
    			newy+=point.y;
			}
    		context.write(new Point(newx/count, newy/count), null);
    	}
    }

    public static void main ( String[] args ) throws Exception {
    
	    Job job = Job.getInstance();
	    job.setJobName("KMeansJob");
	    job.setJarByClass(KMeans.class);
	    
	    job.setOutputKeyClass(Point.class);
	    job.setOutputValueClass(Point.class);
	    
	    job.setMapOutputKeyClass(Point.class);
	    job.setMapOutputValueClass(Point.class);
	    
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
