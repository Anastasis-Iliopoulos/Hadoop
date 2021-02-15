package org.anasis.groupandaggregation;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupAndAggregation {

    public static class GroupAndAggregationMapper extends Mapper<Object, Text, LongWritable, DoubleWritable> {

    	private final LongWritable keyWord = new LongWritable();
        private final DoubleWritable valueWord = new DoubleWritable();
        private final String delimiter = ",";

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String line = value.toString();
        	if(!line.contains("movieId")) {
        		StringTokenizer tokenizer = new StringTokenizer(line,delimiter);
        		String token = tokenizer.nextToken();
        		token = tokenizer.nextToken();
        		keyWord.set(Long.parseLong(token));
        		token = tokenizer.nextToken();
        		valueWord.set(Double.parseDouble(token));
        		context.write(keyWord,valueWord);
        	}
        }
    }

    public static class GroupAndAggregationReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, Text> {

        private Text valueWord = new Text();
        
        @Override
        protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        	
        	double sum = 0.0;
        	int counter = 0;
        	double max=0.0;
        	for ( DoubleWritable tmp : values){
        		counter++;
        		if(max<tmp.get()) {
        			max = tmp.get();
        		}
        		sum = tmp.get() + sum;
            }
            
        	double avg =sum/counter;
        	String valueString =String.valueOf(avg) +  "\t" + String.valueOf(max);
        	
            valueWord.set(valueString);
            context.write(key, valueWord);
        }
    }
    
    public static void main(String[] args) throws Exception {
    	if(args.length != 2) {
    		System.err.println("Wrong number of arguments!");
    		System.err.println("Usage: <input-dir> <output-dir>");
    		System.exit(1);
    	}
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Group And Aggregation");
        job.setJarByClass(GroupAndAggregation.class);
        job.setMapperClass(GroupAndAggregationMapper.class);
        job.setReducerClass(GroupAndAggregationReducer.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
