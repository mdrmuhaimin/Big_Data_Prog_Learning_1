// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 
// package ca.sfu.whatever;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.StringTokenizer;
import java.lang.Object;
import java.text.Normalizer;

import java.util.Locale;
import java.text.BreakIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

 
public class RedditAverage extends Configured implements Tool {
 
	/**
	 * A Writable that represents a pair of long values.
	 */
	public static class LongPairWritable implements Writable {
		private long a;
		private long b;
		private LongWritable writ = new LongWritable();

		public LongPairWritable(long a, long b) {
			this.a = a;
			this.b = b;
		}

		public LongPairWritable() {
			this(0, 0);
		}
		
		public long get_0() {
			return a;
		}
		public long get_1() {
			return b;
		}
		public void set(long a, long b) {
			this.a = a;
			this.b = b;		
		}

		public void write(DataOutput out) throws IOException {
			writ.set(a);
			writ.write(out);
			writ.set(b);
			writ.write(out);
		}
		public void readFields(DataInput in) throws IOException {
			writ.readFields(in);
			a = writ.get();
			writ.readFields(in);
			b = writ.get();
		}
		
		public String toString() {
			return "(" + Long.toString(a) + "," + Long.toString(b) + ")";
		}

	}
	
	public static class SubRedditScoreMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{
 
        private Text word = new Text();
 
        @Override
        public void map(LongWritable key, Text value, Context context
            ) throws IOException, InterruptedException {
            ObjectMapper json_mapper = new ObjectMapper();
            JsonNode data = json_mapper.readValue(value.toString(), JsonNode.class);
            word.set(data.get("subreddit").textValue());
            LongPairWritable pair = new LongPairWritable();
            pair.set(1, data.get("score").longValue());
            context.write(word, pair);
        }
    }
 
    public static class SubRedditScoreReducer
    extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long count = 0;
            long sum = 0;
            for (LongPairWritable val : values) {
            	count += val.get_0();
            	sum += val.get_1();
            }
            result.set((double)sum/count);
            context.write(key, result);
        }
    }
    
    public static class SubRedditScoreCombiner
    extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long count = 0;
            long sum = 0;
            for (LongPairWritable val : values) {
            	count += val.get_0();
            	sum += val.get_1();
            }
            LongPairWritable pair = new LongPairWritable();
            pair.set(count, sum);
            context.write(key, pair);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(RedditAverage.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(SubRedditScoreMapper.class);
        job.setCombinerClass(SubRedditScoreCombiner.class);
        job.setReducerClass(SubRedditScoreReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongPairWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
