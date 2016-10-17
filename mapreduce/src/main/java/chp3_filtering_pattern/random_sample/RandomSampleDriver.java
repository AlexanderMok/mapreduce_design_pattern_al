package chp3_filtering_pattern.random_sample;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

public class RandomSampleDriver
{
	public static class SRSMapper extends Mapper<Object, Text, NullWritable, Text>
	{

		private Random rands = new Random();
		private Double percentage;

		@Override
		protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException
		{
			// Retrieve the percentage that is passed in via the configuration
			// like this: conf.set("filter_percentage", .5);
			// for .5%
			String strPercentage = context.getConfiguration().get("filter_percentage");
			percentage = Double.parseDouble(strPercentage) / 100.0;
		}

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException
		{
			if (rands.nextDouble() < percentage)
			{
				context.write(NullWritable.get(), value);
			}
		}

	}

	public static void main(String[] args) throws IOException
	{
		Configuration conf = new Configuration();
		conf.set("filter_percentage", ".5");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: RandomSampleDriver <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Random Sample Filtering");

	}
}
