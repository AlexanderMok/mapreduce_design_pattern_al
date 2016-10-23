package chp3_filtering_pattern.distinct;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MPRutil;

/**
 * Problem: Given a list of user’s comments, determine the distinct set of user
 * IDs.
 * 
 * @author Alex
 *
 */
public class DistinctDriver
{
	/**
	 * The Mapper will get the user ID from each input record. This user ID will
	 * be output as the key with a null value.
	 * 
	 * @author Alex
	 *
	 */
	public static class DistinctUserMapper extends Mapper<Object, Text, Text, NullWritable>
	{
		private Text outUserId = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Map<String, String> parsed = MPRutil.transformXmlToMap(value.toString());
			// Get the value for the UserId attribute
			String userId = parsed.get("UserId");
			// Set our output key to the user's id
			outUserId.set(userId);
			// Write the user's id with a null value
			context.write(outUserId, NullWritable.get());
		}
	}
	
	/**
	 * The grunt work of building a distinct set of user IDs is handled by the 
	 * MapReduce framework. Each reducer is given a unique key and a set of null values.
	 * These values are ignored and the input key is written to the file system with a null value.
	 * @author Alex
	 *
	 */
	public static class DistinctUserReducer extends Reducer<Text, NullWritable, Text, NullWritable>
	{
		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException
		{
			// Write the user's id with a null value
			context.write(key, NullWritable.get());
		}
	}
	
	
	
	public static void main(String[] args)
	{
        //The same code for the reducer can be used in the combiner.
	}

}
