package org.anasis.innerjoin2;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InnerJoin2 {

    public static class InnerJoin2Mapper extends Mapper<Object, Text, Text, Text> {

    	private final Text keyWord = new Text();
        private final Text valueWord = new Text();
        private final String delimiter = ",";
        private final String separator = FileSystems.getDefault().getSeparator();
        private static final String COMEDY = "C";
        private static final String THREE = "T";
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String line = value.toString();
        	String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
            String fileName = filePathString.substring(filePathString.lastIndexOf(separator)+1);
        	
        	if(!line.contains("movieId")) {
        		
        		if (fileName.contains("ratings")) {
        			StringTokenizer tokenizer = new StringTokenizer(line,delimiter);
        			String token = tokenizer.nextToken();
        			token = tokenizer.nextToken();
        			keyWord.set(token);
        			token = tokenizer.nextToken();
        			if(Double.parseDouble(token)>4.0){
                        valueWord.set(THREE);
                        context.write(keyWord,valueWord);
                    }
        			
        		}
        		if (fileName.contains("movies")) {
        			StringTokenizer tokenizer = new StringTokenizer(line,delimiter);
        			String token = tokenizer.nextToken();
        			keyWord.set(token);
        			
        			if(line.substring(line.lastIndexOf(delimiter)+1).toLowerCase().contains("Comedy".toLowerCase())) {
        				valueWord.set(COMEDY);
        				context.write(keyWord,valueWord);
        			}
        			valueWord.set(line.substring(line.indexOf(delimiter)+1, line.lastIndexOf(delimiter)));
        			context.write(keyWord,valueWord);
        		}
        		
        	}
        }
    }
    public static class InnerJoin2Reducer extends Reducer<Text, Text, NullWritable, Text> {

    	private final Set<String> valuesSet = new LinkedHashSet<>();
        private Text valueWord = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	valuesSet.clear();
        	for ( Text tmp : values){
                valuesSet.add(tmp.toString());
            }
            
        	if(valuesSet.contains("C") || valuesSet.contains("T")) {
        		for ( String tmp : valuesSet){
                	if((!tmp.equals("C")) && (!tmp.equals("T"))) {
                		
                		valueWord.set(tmp);
                		context.write(NullWritable.get(),valueWord);
                	}
                }
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
        Job job = Job.getInstance(conf, "Inner Join2");
        job.setJarByClass(InnerJoin2.class);
        job.setMapperClass(InnerJoin2Mapper.class);
        job.setReducerClass(InnerJoin2Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
