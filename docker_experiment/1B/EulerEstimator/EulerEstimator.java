// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 
// package ca.sfu.whatever;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.StringTokenizer;
import java.lang.Object;
import java.text.Normalizer;

import java.util.Locale;
import java.util.Random;
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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class EulerEstimator extends Configured implements Tool {
 
	public static class CounterMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
        private Text word = new Text();
 
        @Override
        public void map(LongWritable key, Text value, Context context
            ) throws IOException, InterruptedException {
        	long iter = Long.parseLong(value.toString());
			long count = 0;
			for (long i = 0; i < iter; i++) {
			    double sum = 0.0;
			    while (sum < 1) {
			    	Random generator = new Random((((FileSplit) context.getInputSplit()).getPath().getName()).hashCode() + key.get());
			        double num = generator.nextDouble();
			        sum += num;
			        count ++;
			    }
			}
			context.getCounter("Euler", "iterations").increment(iter);
			context.getCounter("Euler", "count").increment(count);
        }
    }
 
     public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new EulerEstimator(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(EulerEstimator.class);
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(CounterMapper.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}