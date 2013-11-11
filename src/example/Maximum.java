package example;

import java.util.Iterator;
import java.util.StringTokenizer;

import mapreduce.IntWritable;
import mapreduce.LongWritable;
import mapreduce.MapReduceBase;
import mapreduce.Mapper;
import mapreduce.OutputCollector;
import mapreduce.Reducer;
import mapreduce.Reporter;
import mapreduce.Text;

public class Maximum {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		private final static Text max = new Text("max");
		private LongWritable number = new LongWritable();

		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output,
				Reporter reporter) throws Exception {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				number.set(tokenizer.nextToken());
				output.collect(max, number);
			}
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws Exception {
			long maxValue = Long.MIN_VALUE;
			while (values.hasNext()) {
			     maxValue = Math.max(maxValue, values.next().get()); 
			}
			output.collect(key, new LongWritable(maxValue));
		}
	}

}
