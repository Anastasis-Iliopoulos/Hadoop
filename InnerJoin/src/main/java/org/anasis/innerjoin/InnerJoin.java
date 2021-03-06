package org.anasis.innerjoin;

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

public class InnerJoin {

    public static class InnerJoinMapper extends Mapper<Object, Text, Text, Text> {

        private final Text keyWord = new Text();
        private final Text valueWord = new Text();
        private final String delimiter = ",";
        
        private final String separator = FileSystems.getDefault().getSeparator();
        
        
        
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
                        valueWord.set("t");
                        context.write(keyWord,valueWord);
                    }
        			
        		}
        		if (fileName.contains("movies")) {
        			StringTokenizer tokenizer = new StringTokenizer(line,delimiter);
        			String token = tokenizer.nextToken();
        			keyWord.set(token);
        			valueWord.set(line.substring(line.indexOf(delimiter)+1, line.lastIndexOf(delimiter)));
        			context.write(keyWord,valueWord);
        		}
        	}
        }
    }

    public static class InnerJoinReducer extends Reducer<Text, Text, NullWritable, Text> {

    	private final Set<String> valuesSet = new LinkedHashSet<>();
        private Text valueWord = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	valuesSet.clear();
        	
        	for ( Text tmp : values){
                valuesSet.add(tmp.toString());
            }
            
        	if(valuesSet.contains("t")) {
        		for ( String tmp : valuesSet){
                	if(!tmp.equals("t")) {
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
        Job job = Job.getInstance(conf, "Inner Join");
        job.setJarByClass(InnerJoin.class);
        job.setMapperClass(InnerJoinMapper.class);
        job.setReducerClass(InnerJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
