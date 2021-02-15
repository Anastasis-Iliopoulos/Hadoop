package org.anasis.selectionandprojection;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SelectionAndProjection {

    public static class SelectionAndProjectionMapper extends Mapper<Object, Text, Text, Text> {

        private final Text keyWord = new Text();
        private final Text valueWord = new Text();
        private final String delimiter = ",";
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String line = value.toString();
        	
            if(!line.contains("movieId")) {
            	StringTokenizer tokenizer = new StringTokenizer(line,delimiter);
            	String token = tokenizer.nextToken();
            	token = tokenizer.nextToken();
            	keyWord.set(token);
            	token = tokenizer.nextToken();
            	if(Double.parseDouble(token)>=3.0){
                    valueWord.set(token);
                    context.write(keyWord,valueWord);
                }
            }
        }
    }

    public static class SelectionAndProjectionReducer extends Reducer<Text, Text, Text, Text> {

    	private final Set<String> valuesSet = new LinkedHashSet<>();
        private Text valueWord = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	valuesSet.clear();
        
        	for ( Text tmp : values){
        		
                valuesSet.add(tmp.toString());
            }
        	
            
            for ( String tmp : valuesSet){
                valueWord.set(tmp);
                context.write(key,valueWord);
            }
        }
    }

    public static void main(String[] args) throws Exception {
    	if(args.length != 2) {
    		System.err.println("Wrong number of arguments!");
    		System.err.println("Usage: <input-dir> <output-dir>");
    		System.exit(1);
    	}
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Selection And Projection");
        job.setJarByClass(SelectionAndProjection.class);
        job.setMapperClass(SelectionAndProjectionMapper.class);
        job.setReducerClass(SelectionAndProjectionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
