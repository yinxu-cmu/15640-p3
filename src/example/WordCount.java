/**
 * 
 */
package example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import mapreduce.*;
import mapreduce.OutputCollector.Entry;

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
		OutputCollector<Text, IntWritable> mapOutput = new OutputCollector<Text, IntWritable>();
		OutputCollector<Text, IntWritable> combineOutput = new OutputCollector<Text, IntWritable>();

		// dummy reporter, no use
		Reporter reporter = new Reporter();
		WordCount.Map mapper = new WordCount.Map();
		WordCount.Reduce combiner = new WordCount.Reduce();
		
		// input value is one line from the input file
		Text inputValue = new Text();
		BufferedReader bufferedReader = new BufferedReader(new FileReader("test.txt"));
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			inputValue.set(line);
			mapper.map(null, inputValue, mapOutput, reporter);
		}
	
//		while (mapOutput.queue.size() != 0)
//			System.out.print(mapOutput.queue.poll() + " ");
		
		// start combine
		mapreduce.OutputCollector.Entry entry = mapOutput.queue.poll();
		mapreduce.OutputCollector.Entry tmpEntry = null; 
		ArrayList<IntWritable> values = new ArrayList<IntWritable>();
		Iterator<IntWritable> itrValues = null;

		Text key = (Text) entry.getKey();
		values.add((IntWritable) entry.getValue());
		
		Method method = key.getClass().getMethod("getHashcode", null);
		int hash = (Integer) method.invoke(key, null);
		int tmpHash = 0;
		while (mapOutput.queue.size() != 0) {
			tmpEntry = mapOutput.queue.poll();
			tmpHash = (Integer) method.invoke(tmpEntry.getKey(), null);
			if (tmpHash == hash) {
				values.add((IntWritable) tmpEntry.getValue());
			} else {
				itrValues = values.iterator();
				combiner.reduce(key, itrValues, combineOutput, reporter);
				entry = tmpEntry;
				key = (Text) entry.getKey();
				hash = (Integer) method.invoke(key, null);
				values = new ArrayList<IntWritable>();
				values.add((IntWritable) entry.getValue());
			}
		}
		
		// don't forget the last one :)
		itrValues = values.iterator();
		combiner.reduce(key, itrValues, combineOutput, reporter);

		System.out.println("debug");
		
//		while (combineOutput.queue.size() != 0)
//			System.out.print(combineOutput.queue.poll() + " ");
		
		
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