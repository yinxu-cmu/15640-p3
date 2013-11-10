/**
 * 
 */
package example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.StringTokenizer;

import mapreduce.*;

public class WordCount {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws Exception{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws Exception{
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		// don't know if this is useful
		WordCount wordCount = new WordCount();
		
		// should know the type of output key and output value from user
		OutputCollector<Text, IntWritable> output = new OutputCollector<Text, IntWritable>();
		
		// dummy reporter, no use
		Reporter reporter = new Reporter();
		
		WordCount.Map mapper = new WordCount.Map();
		
		// input value is one line from the input file
		Text inputValue = new Text();
		BufferedReader bufferedReader = new BufferedReader(new FileReader("test.txt"));
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			inputValue.set(line);
			mapper.map(null, inputValue, output, reporter);
		}
	
 		System.out.println(output.map.size());
		
		// JobConf conf = new JobConf(WordCount.class);
		// conf.setJobName("wordcount");
		//
		// conf.setOutputKeyClass(Text.class);
		// conf.setOutputValueClass(IntWritable.class);
		//
		// conf.setMapperClass(Map.class);
		// conf.setCombinerClass(Reduce.class);
		// conf.setReducerClass(Reduce.class);
		//
		// conf.setInputFormat(TextInputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);
		//
		// FileInputFormat.setInputPaths(conf, new Path(args[0]));
		// FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		//
		// JobClient.runJob(conf);
	}
}