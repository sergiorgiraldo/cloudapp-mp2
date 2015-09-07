import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
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

    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = this.getConf();
    	FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

    	Job jobA = Job.getInstance(conf, "Popularity Count");
    	jobA.setOutputKeyClass(IntWritable.class);
    	jobA.setOutputValueClass(IntWritable.class);

    	jobA.setMapperClass(LinkCountMap.class);
    	jobA.setReducerClass(LinkCountReduce.class);

    	FileInputFormat.setInputPaths(jobA, new Path(args[0]));
    	FileOutputFormat.setOutputPath(jobA, tmpPath);
    	//FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

    	jobA.setJarByClass(PopularityLeague.class);
    	jobA.waitForCompletion(true);

    	Job jobB = Job.getInstance(conf, "Popularity League");
    	jobB.setOutputKeyClass(IntWritable.class);
    	jobB.setOutputValueClass(IntWritable.class);

    	jobB.setMapOutputKeyClass(NullWritable.class);
    	jobB.setMapOutputValueClass(IntArrayWritable.class);

    	jobB.setMapperClass(LeagueLinksMapper.class);
    	jobB.setReducerClass(LeagueLinksReducer.class);

    	FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
    	return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    // TODO
    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
    	Set<String> leagueSet;
    	@Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String league_file = conf.get("league");
            this.leagueSet = new HashSet<String>(Arrays.asList(readHDFSFile(league_file, conf).split("\n")));
        }

    	@Override
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		String line = value.toString();
    		String delimiters = " :";
    		StringTokenizer tokenizer = new StringTokenizer(line, delimiters);
    		if (tokenizer.hasMoreTokens()) {
    			tokenizer.nextToken();
    		}
    		while (tokenizer.hasMoreElements()) {
    			Integer to = Integer.parseInt(tokenizer.nextToken().trim());
    			if (this.leagueSet.contains(to.toString())) {
    			context.write(new IntWritable(to), new IntWritable(1));
    			}
    		}
    	}
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    	@Override
    	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		int count = 0;
    		for (IntWritable val: values) {
    			count += val.get();
    		}
    		context.write(key, new IntWritable(count));
    	}
    }

    public static class LeagueLinksMapper extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
    	@Override
    	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    		Integer[] kv = {Integer.parseInt(key.toString()), Integer.parseInt(value.toString())};
            IntArrayWritable val = new IntArrayWritable(kv);
            context.write(NullWritable.get(), val);

    	}
    }

    public static class LeagueLinksReducer extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
    	private ArrayList<Pair<Integer, Integer>> league = new ArrayList<Pair<Integer, Integer>>();
    	@Override
    	public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
    		for (IntArrayWritable val: values) {
            	IntWritable[] pair = (IntWritable[]) val.toArray();

            	Integer link = pair[0].get();
            	Integer count = pair[1].get();

            	league.add(new Pair(link, count));
            }

    		if (league.size() == 0) {
    			return;
    		}
    		Collections.sort(league, new Comparator<Pair<Integer, Integer>>() {
            	@Override
            	public int compare(Pair<Integer, Integer> a, Pair<Integer, Integer> b) {
            		return a.second.compareTo(b.second);
            	}
            });

    		int rank = 0, prev_popularity = league.get(0).second;
    		context.write(new IntWritable(league.get(0).first), new IntWritable(rank));

    		for (int i = 1; i < league.size(); i++) {
    			Pair<Integer, Integer> curr = league.get(i);
    			if (curr.second > prev_popularity) {
    				rank = i;
    				prev_popularity = curr.second;
    			}
    			context.write(new IntWritable(curr.first), new IntWritable(rank));
    		}
    	}
    }

}


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
