package org.anasis.intersection;

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

public class Intersection {

    public static class IntersectionMapper extends Mapper<Object, Text, Text, Text> {

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
            		if(Double.parseDouble(token)>=3.0){
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
            	}
            }
        }
    }
    
    public static class IntersectionReducer extends Reducer<Text, Text, Text, NullWritable> {

    	private final Set<String> valuesSet = new LinkedHashSet<>();
        private static final String COMEDY = "C";
        private static final String THREE = "T";
        
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	valuesSet.clear();
        	for ( Text tmp : values){
                valuesSet.add(tmp.toString());
            }
        	if(valuesSet.contains(COMEDY) && valuesSet.contains(THREE)) {
        		context.write(key,NullWritable.get());
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
        Job job = Job.getInstance(conf, "Intersection");
        job.setJarByClass(Intersection.class);
        job.setMapperClass(IntersectionMapper.class);
        job.setReducerClass(IntersectionReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
