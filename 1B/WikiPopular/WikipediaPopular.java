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
 
public class WikipediaPopular extends Configured implements Tool {
 
    public static class PageHitMapper
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
            		String[] fileNameComponents = fileName.split("-");
                    String timeHour = fileNameComponents[fileNameComponents.length - 2] + "-" + fileNameComponents[fileNameComponents.length - 1].substring(0,2);
                    word.set(timeHour);
            		context.write(word, pageHit);
        		}
    		}
    }
 
    public static class MaxPageCountReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long max = 0;
            for (LongWritable val : values) {
                if ( val.get() >= max) {
                    max = val.get();
                }
            }
            result.set(max);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WikipediaPopular.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(PageHitMapper.class);
        job.setCombinerClass(MaxPageCountReducer.class);
        job.setReducerClass(MaxPageCountReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
