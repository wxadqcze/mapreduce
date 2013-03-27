/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name:
 * Partner 1 Login:
 *
 * Partner 2 Name:
 * Partner 2 Login:
 *
 * REMINDERS: 
 *
 * 1) YOU MUST COMPLETE THIS PROJECT WITH A PARTNER.
 * 
 * 2) DO NOT SHARE CODE WITH ANYONE EXCEPT YOUR PARTNER.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 10;

    // Example writable type
    public static class EValue implements Writable {

        public LongWritable id;
	public LongWritable source;
	public int distance;
        public ArrayList<LongWritable> children;
	public boolean toSearch;

        public EValue(LongWritable id, LongWritable source, int distance, 
		      ArrayList<LongWritable> children, boolean toSearch) {
	    this.id = id;
            this.source = source;
	    this.distance = distance;
	    this.children = children;
	    this.toSearch = toSearch;
        }

	public EValue(LongWritable id, ArrayList<LongWritable> children) {
	    this.id = id;
	    this.source = new LongWritable(-1);
	    this.distance = -1;
	    this.children = children;
	    this.toSearch = false;
	}

        public EValue() {
            // does nothing
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
	    out.writeLong(id.get());
	    out.writeLong(source.get());
            out.writeInt(distance);
	    out.writeBoolean(toSearch);

            int length = 0;
            if (children != null){
                length = children.size();
            }
            out.writeInt(length);

            // now write each long in the array
            for (int i = 0; i < length; i++){
                out.writeLong(children.get(i).get());
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
	    id = new LongWritable(in.readLong());
	    source = new LongWritable(in.readLong());
            distance = in.readInt();
	    toSearch = in.readBoolean();

            int length = in.readInt();
            children = new ArrayList<LongWritable>(length);
            
            for(int i = 0; i < length; i++){
                children.add(i, new LongWritable(in.readLong()));
            }
        }

        public String toString() {
            // We highly recommend implementing this for easy testing and
            // debugging. This version just returns an empty string.
            return id + "/" + source + "/" + distance + "/" 
		+ children + "/" + toSearch;
        }

    }


    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            context.write(key, value);
	    context.write(value, new LongWritable(Long.MAX_VALUE));
        }
    }


    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, EValue> {

        public long denom;

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {

            denom = Long.parseLong(context.getConfiguration().get("denom"));

	    int rand = new Double(Math.random() * denom).intValue();

	    // populate list of children
	    ArrayList<LongWritable> children = new ArrayList<LongWritable>();
	    for (LongWritable child : values) {
		long cLong = child.get();
		LongWritable c = new LongWritable(cLong);
		if (cLong != Long.MAX_VALUE) {
		    children.add(c);
		}
	    }

	    // default EValue for all nodes
	    EValue node = new EValue(key, children);

	    // if it's a source node, we add more info and mark to be searched
	    if (rand <= 1) {
		node = new EValue(key, key, 0, children, true);
	    }
	    
	    context.write(key, node);

        }

    }


    // ------- Add your additional Mappers and Reducers Here ------- //


    /* BFSMAP*/
    public static class BFSMapper extends Mapper<LongWritable, EValue, 
					  LongWritable, EValue> {

        @Override
        public void map(LongWritable key, EValue value, Context context)
                throws IOException, InterruptedException {

            int dist = Integer.parseInt(context.getConfiguration().get("loopCount")) + 1;

	    if (value.toSearch) {
		// mark parent node as searched, emit it
		value.toSearch = false;
		context.write(key, value);
		// pass on an evalue with each child's information
		for (LongWritable childID : value.children) {
		    EValue childNode = new EValue(childID, value.source, dist, 
						  new ArrayList<LongWritable>(),
						  true);
		    context.write(childID, childNode);
		}
	    } else {
		context.write(key, value);
	    }
	    
        }
    }



    /* BFSREDUCE */
    public static class BFSReducer extends Reducer<LongWritable, EValue, 
        LongWritable, EValue> {

        public void reduce(LongWritable key, Iterable<EValue> values, 
            Context context) throws IOException, InterruptedException {

	    HashMap<Long, EValue> t = new HashMap<Long, EValue>();
	    ArrayList<LongWritable> children = new ArrayList<LongWritable>();
	    ArrayList<EValue> valueList = new ArrayList<EValue>();

	    for (EValue value : values) {
		EValue temp = new EValue(value.id, value.source, value.distance,
					 value.children, value.toSearch);
		valueList.add(temp);
	    }

	    for (EValue value : valueList) {
		
		if (!value.toSearch) {
		    children = value.children;
		}

		if (t.containsKey(value.source.get())) {
		    EValue temp = t.get(value.source.get());
		    if (value.distance < temp.distance) {
			temp = t.put(value.source.get(), value);
		    }
		} else {
		    t.put(value.source.get(), value);
		}
	    }

	    for (EValue child : t.values()) {
		child.children = children;
		context.write(key, child);
	    }
	}
    }





    /* HistMap*/
    public static class HistMap extends Mapper<LongWritable, EValue, 
        LongWritable, LongWritable> {

	public static LongWritable ONE = new LongWritable(1);


        @Override
        public void map(LongWritable key, EValue value, Context context)
                throws IOException, InterruptedException {

	    if (value.distance != -1) {
		context.write(new LongWritable(value.distance), ONE);
	    }
	    
        }
    }



    /* HistReduce */
    public static class HistReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {

	    long total = 0;
            for (LongWritable value : values){
		total += value.get();
            }
	    context.write(key, new LongWritable(total));
        }

    }




    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);

        // Sample of passing value from main into Mappers/Reducers using
        // conf. You might want to use something like this in the BFS phase:
        // See LoaderMap for an example of how to access this value
        conf.set("inputValue", (new Integer(5)).toString());

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
	job.setNumReduceTasks(24);
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(EValue.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Repeats your BFS mapreduce
        int i = 0;
        while (i < MAX_ITERATIONS) {
	    conf.set("loopCount", Integer.toString(i));
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);
            job.setNumReduceTasks(24);
            // Feel free to modify these four lines as necessary:
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(EValue.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(EValue.class);

            // You'll want to modify the following based on what you call
            // your mapper and reducer classes for the BFS phase.
            job.setMapperClass(BFSMapper.class); 
            job.setReducerClass(BFSReducer.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);
        job.setNumReduceTasks(1);
        // Feel free to modify these two lines as necessary:
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // DO NOT MODIFY THE FOLLOWING TWO LINES OF CODE:
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // You'll want to modify the following based on what you call your
        // mapper and reducer classes for the Histogram Phase
        job.setMapperClass(HistMap.class); // currently the default Mapper
        job.setReducerClass(HistReduce.class); // currently the default Reducer
	
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
