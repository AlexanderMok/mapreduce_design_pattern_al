package chp1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount
{

	public static class WordMapper extends Mapper<Object, Text, Text, IntWritable>
	{

		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			// Now typically NLP pre-process.
			
			

			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens())
			{
				word.set(tokenizer.nextToken());
				// each word counted as one
				context.write(word, one);
			}
		}
	}

	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable count : values)
			{
				sum = sum + count.get();
			}

			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception
	{
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
		    System.exit(2);
		}
		
		// Create a new Job
		Job job = Job.getInstance(conf, "Word_Count");
		job.setJarByClass(WordCount.class);
		
		// Specify various job-specific parameters
		job.setMapperClass(WordMapper.class);
		job.setCombinerClass(CountReducer.class);
		job.setReducerClass(CountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// Submit the job, then poll for progress until the job is complete
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
