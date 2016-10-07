package chp2_summarization_pattern.numerical_sum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import utils.MPRutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



/**
 * Problem: Given a list of user’s comments, determine the median and standard deviation 
 * of comment lengths per hour of day.
 * @author Alex
 *
 */
public class MedianStdDevDriver
{

	public static class MedianStdDevMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable>
	{

		private IntWritable outHour = new IntWritable();
		private IntWritable commentLength = new IntWritable();
		private static final LongWritable ONE = new LongWritable(1);

		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@SuppressWarnings("deprecation")
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{

			// Parse the input string into a nice map
			Map<String, String> parsed = MPRutil.transformXmlToMap(value.toString());

			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");

			// Grab the comment to find the length
			String text = parsed.get("Text");

			// .get will return null if the key is not there
			if (strDate == null || text == null)
			{
				// skip this record
				return;
			}

			try
			{
				// get the hour this comment was posted in
				Date creationDate = frmt.parse(strDate);
				outHour.set(creationDate.getHours());

				commentLength.set(text.length());
				SortedMapWritable outCommontLength = new SortedMapWritable();
				outCommontLength.put(commentLength, ONE);

				// write out the user ID with min max dates and count
				context.write(outHour, outCommontLength);

			} catch (ParseException e)
			{
				System.err.println(e.getMessage());
				return;
			}
		}
	}

	public static class MedianStdDevReducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple>
	{
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private TreeMap<Integer,Long> commentLengthsCount = new TreeMap<>();

		@Override
		public void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException
		{

			float sum = 0;
			long totalComments = 0;
			
			commentLengthsCount.clear();
			result.setStdDev(0);
			result.setMedian(0);

			// Iterate through all input values for this key
			for (SortedMapWritable val : values)
			{
				for (Map.Entry<WritableComparable, Writable> entry : val.entrySet())
				{
					int length = ((IntWritable)entry.getKey()).get();
					long count = ((LongWritable)entry.getValue()).get();
					
					//计算该小时的总输入值数目
					totalComments += count;
					
					//计算该小时的总流量
					sum += count * length;
					
					Long storedCount = commentLengthsCount.get(length);
					
					// 保存每一个流量值和相应的出现次数
					if (storedCount == null)
					{
						commentLengthsCount.put(length, count);
					}
					else 
					{
						commentLengthsCount.put(length, storedCount + count);
					}
				}	
			}
			
			// 计算中值的位置索引
			long medianIndex = totalComments / 2L;
			// 保存遍历过程中上一次的位置索引
			long previousComments = 0;
			// 当前位置索引
			long comments = 0;
			// 上一次遍历的key
			int prevKey = 0;
			for (Map.Entry<Integer, Long> entry : commentLengthsCount.entrySet())
			{
				comments = previousComments + entry.getValue();
				
				//如果中值位置索引大于前一个位置索引，并且小于当前位置索引，那么中值就在里面
				if (previousComments <= medianIndex && medianIndex < comments)
				{
					//如果中值索引是双数，并且等于上一次遍历的索引，那么中值就是两个位置的值的平均数
					if (totalComments % 2 == 0 && previousComments == medianIndex)
					{
						result.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
					} 
					else
					{
						result.setMedian(entry.getKey());
					}
					break;
				}
				previousComments = comments;
				prevKey = entry.getKey();
			}
			
			// calculate standard deviation
			float mean = sum / totalComments;
			float sumOfSquares = 0.0f;
			for (Map.Entry<Integer, Long> entry : commentLengthsCount.entrySet())
			{
				sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
			}
			
			result.setStdDev((float) Math.sqrt(sumOfSquares / (totalComments - 1)));

			context.write(key, result);
		}
	}
	
	public static class MedianStdDevCombiner extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>
	{
		@Override
		protected void reduce(IntWritable key, Iterable<SortedMapWritable> values,
				Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>.Context context)
				throws IOException, InterruptedException
		{
			SortedMapWritable outValue = new SortedMapWritable();
			for (SortedMapWritable v : values)
			{
				for (Map.Entry<WritableComparable, Writable> entry : v.entrySet())
				{
				    LongWritable count = (LongWritable)outValue.get(entry.getKey());
				    
				    if (count != null)
					{
				    	count.set(count.get()
				    			+ ((LongWritable) entry.getValue()).get());
					} 
				    else
					{
				    	outValue.put(entry.getKey(), new LongWritable(
				    			((LongWritable) entry.getValue()).get()));
					}
				}
			}
			
			context.write(key, outValue);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: MedianStdDevDriver <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "StackOverflow Comment Length Median StdDev By Hour");
		
		job.setJarByClass(MedianStdDevDriver.class);
		
		job.setMapperClass(MedianStdDevMapper.class);
		job.setCombinerClass(MedianStdDevCombiner.class);
		job.setReducerClass(MedianStdDevReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MedianStdDevTuple.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MedianStdDevTuple implements Writable
	{
		private float median = 0;
		private float stddev = 0f;

		public float getMedian()
		{
			return median;
		}

		public void setMedian(float median)
		{
			this.median = median;
		}

		public float getStdDev()
		{
			return stddev;
		}

		public void setStdDev(float stddev)
		{
			this.stddev = stddev;
		}

		@Override
		public void readFields(DataInput in) throws IOException
		{
			median = in.readFloat();
			stddev = in.readFloat();
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeFloat(median);
			out.writeFloat(stddev);
		}

		@Override
		public String toString()
		{
			return median + "\t" + stddev;
		}
	}
}
