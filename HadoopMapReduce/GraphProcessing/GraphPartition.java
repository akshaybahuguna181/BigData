import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    /* ... */
    
    Vertex(){}
    
    Vertex(long id, Vector<Long> adjacent, long centroid, short depth){
    	this.id = id;
    	this.adjacent = adjacent;
    	this.centroid = centroid;
    	this.depth = depth;
    }
    
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeLong(centroid);
		out.writeShort(depth);
		
		int size = adjacent.size();
		out.writeInt(adjacent.size());
		
		for(int i=0; i<size; i++) {
			out.writeLong(adjacent.get(i));
		}
	}
	
	public void readFields(DataInput in) throws IOException {
		id = in.readLong();
		centroid = in.readLong();
		depth = in.readShort();
		
		adjacent = new Vector<Long>();
		int size = in.readInt();
		for(int i=0;i<size;i++) {
			adjacent.add(in.readLong());
		}
	}

	@Override
	public String toString() {
		return "Vertex [id=" + id + ", adjacent=" + adjacent + ", centroid=" + centroid + ", depth=" + depth + "]";
	}
	
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    
    static int count = 0;
    /* ... */
public static class FirstMapperClass extends Mapper<Object,Text,LongWritable,Vertex> {
    	
    	@Override
    	public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
    		Scanner s = new Scanner(value.toString()).useDelimiter(",");
    		long vertexId = s.nextLong();
    		Vertex vertex = new Vertex();
    		vertex.id = vertexId;
    		vertex.adjacent = new Vector<Long>();
    		vertex.depth = (short) 0;
    		
    		if(count<10) {
    			System.out.println("Centroid is "+vertexId);
    			vertex.centroid = vertexId;
    		}else {
    			System.out.println("Unassigned vertices: "+vertexId);
    			vertex.centroid = -1;
    		}

    		while(s.hasNext()){
    			vertex.adjacent.add(s.nextLong());
    		}
    		
    		count++;
    		context.write(new LongWritable(vertexId), vertex);
    		s.close();
    	}
    }

public static class SecondMapperClass extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
	
	@Override
	public void map(LongWritable id, Vertex vertex, Context context ) throws IOException, InterruptedException {
		
		context.write(new LongWritable(vertex.id), vertex);
		
		if(vertex.centroid>0){
			Iterator<Long> neighbours = vertex.adjacent.iterator();
			
			while(neighbours.hasNext()) {
				long adjId = neighbours.next();
				context.write(new LongWritable(adjId), new Vertex(adjId, new Vector<Long>(), vertex.centroid, BFS_depth));;
			}
		}
		
	}
}

public static class FirstReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
	
	@Override
	public void reduce(LongWritable id, Iterable<Vertex> vertices, Context context) throws IOException, InterruptedException {
		
		short minDepth = 1000;
		
		Vertex temp = new Vertex(id.get(), new Vector<Long>(), -1, (short) 0);
		
		for(Vertex v: vertices) {
			if(!v.adjacent.isEmpty()) {
				temp.adjacent = v.adjacent;
			}
			if(v.centroid>0 && v.depth<minDepth) {
				temp.centroid = v.centroid; 
				minDepth = v.depth;
			}
		}
		temp.depth = minDepth;
		
		context.write(id, temp);
	}
}

public static class ThirdMapperClass extends Mapper<LongWritable,Vertex,LongWritable,IntWritable> {
	
	@Override
	public void map(LongWritable id, Vertex vertex, Context context ) throws IOException, InterruptedException {
	//	context.write(id, new IntWritable(1));
		context.write(new LongWritable(vertex.centroid), new IntWritable(1));
	}
}

public static class SecondReducer extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {
	
	@Override
	public void reduce(LongWritable id, Iterable<IntWritable> valueCounts, Context context) throws IOException, InterruptedException {
		int start = 0;
		for(IntWritable i: valueCounts) {
			start += i.get();
		}
		context.write(id, new IntWritable(start));
	}
}

    public static void main ( String[] args ) throws Exception {
    	
    	/* ... First Map-Reduce job to read the graph */
        Job job = Job.getInstance();
        job.setJobName("1st Job");
        
        job.setJarByClass(GraphPartition.class);
        
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Vertex.class);
	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Vertex.class);
	    
	    job.setMapperClass(FirstMapperClass.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
        
            /* ... Second Map-Reduce job to do BFS */
            job = Job.getInstance();
            job.setJobName("2nd Job");
            job.setJarByClass(GraphPartition.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            
    	    job.setOutputKeyClass(LongWritable.class);
    	    job.setOutputValueClass(Vertex.class);
    	    
    	    job.setMapOutputKeyClass(LongWritable.class);
    	    job.setMapOutputValueClass(Vertex.class);
    	    
    	    job.setMapperClass(SecondMapperClass.class);
    	    job.setReducerClass(FirstReducer.class);

    	    SequenceFileInputFormat.setInputPaths(job,new Path(args[1]+"/i"+i));
    	    SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));
            job.waitForCompletion(true);
        }
        
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job = Job.getInstance();
        job.setJobName("3rd Job");
        job.setJarByClass(GraphPartition.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(ThirdMapperClass.class);
	    job.setReducerClass(SecondReducer.class);
	    SequenceFileInputFormat.setInputPaths(job,new Path(args[1]+"/i8"));
	    FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
