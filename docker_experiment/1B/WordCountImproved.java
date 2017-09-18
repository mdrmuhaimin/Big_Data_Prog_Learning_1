// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 
// package ca.sfu.whatever;
 
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Object;
import java.text.Normalizer;

import java.util.Locale;
import java.text.BreakIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
 
public class WordCountImproved extends Configured implements Tool {
 
    public static class TokenizerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
        private static LongWritable pageHit;
        private Text word = new Text();
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        		String line =  value.toString();
        		String[] wikiParts = line.split(" ");
        		if(line.startsWith("en") && !wikiParts[1].startsWith("Special:") && !wikiParts[1].equals("Main_Page") ) {
            		pageHit =  new LongWritable(new Long(wikiParts[2]).longValue());
            		word.set(fileName);
            		context.write(word, pageHit);
        		}
    		}
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountImproved.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
