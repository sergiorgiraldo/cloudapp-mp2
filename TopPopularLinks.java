import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Comparator;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = this.getConf();
    	FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);
        
    	Job jobA = Job.getInstance(conf, "Link Count");
 
    	jobA.setOutputKeyClass(IntWritable.class);
    	jobA.setOutputValueClass(IntWritable.class);
    	    	    	
    	jobA.setMapperClass(LinkCountMap.class);
    	jobA.setReducerClass(LinkCountReduce.class);
    	
    	FileInputFormat.setInputPaths(jobA, new Path(args[0]));
    	FileOutputFormat.setOutputPath(jobA, tmpPath);
        
    	jobA.setJarByClass(TopPopularLinks.class);
    	jobA.waitForCompletion(true);
    	
    	Job jobB = Job.getInstance(conf, "Top Popular Links");
    	jobB.setOutputKeyClass(IntWritable.class);
    	jobB.setOutputValueClass(IntWritable.class);
    	
    	jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);
    	
    	jobB.setMapperClass(TopLinksMap.class);
    	jobB.setReducerClass(TopLinksReduce.class);
    	
    	FileInputFormat.setInputPaths(jobB, tmpPath);
    	FileOutputFormat.setOutputPath(jobB, new Path(args[1]));
    	
    	jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

    	jobB.setJarByClass(TopPopularLinks.class);
    	return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
    	@Override
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		String delimiters = " :";
    		String line = value.toString(); 
    		StringTokenizer tokenizer = new StringTokenizer(line, delimiters);
    		if (tokenizer.hasMoreTokens()) {
    			Integer from = Integer.parseInt(tokenizer.nextToken().trim());
    			context.write(new IntWritable(from), new IntWritable(0));
    		}
    	
    		while (tokenizer.hasMoreTokens()) {
    			Integer to = Integer.parseInt(tokenizer.nextToken().trim());        		
    			context.write(new IntWritable(to), new IntWritable(1));
    		}
    	}
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // DONE
    	@Override
    	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		int sum = 0;
    		for (IntWritable val: values) {
    			sum += val.get();
    		}
    		context.write(key, new IntWritable(sum));
    	}
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        // DONE
        private TreeSet<Pair<Integer, Integer>> countToIdMap = new TreeSet<Pair<Integer, Integer>>();
        
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Integer count = Integer.parseInt(value.toString());
        	Integer id = Integer.parseInt(key.toString());
        	countToIdMap.add(new Pair<Integer, Integer> (count, id));
        	
        	if (countToIdMap.size() > this.N) {
        		countToIdMap.remove(countToIdMap.first());
        	}
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> item : countToIdMap) {
                Integer[] strings = {item.second, item.first};
                IntArrayWritable val = new IntArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        private TreeSet<Pair<Integer, Integer>> countToIdMap = new TreeSet<Pair<Integer, Integer>>();
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (IntArrayWritable val: values) {
            	IntWritable[] pair = (IntWritable[]) val.toArray();
            	
            	Integer count = pair[0].get();
            	Integer id = pair[1].get();
            	
            	countToIdMap.add(new Pair<Integer, Integer>(count, id));
            	
            	if (countToIdMap.size() > this.N) {
            		countToIdMap.remove(countToIdMap.first());
            	}            	
            }
            
            ArrayList<Pair<Integer, Integer>>  result = new ArrayList<Pair<Integer, Integer>>();
            
            for (Pair<Integer, Integer> item: countToIdMap) {
            	result.add(item);
            }
            Collections.sort(result, new Comparator<Pair<Integer, Integer>>() {
            	@Override
            	public int compare(Pair<Integer, Integer> a, Pair<Integer, Integer> b) {
            		return a.second.compareTo(b.second);
            	}
            });
            
            for (Pair<Integer, Integer> item: result) {
            	IntWritable count = new IntWritable(item.first);
            	IntWritable id = new IntWritable(item.second);
            	context.write(count, id);
            }
        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
