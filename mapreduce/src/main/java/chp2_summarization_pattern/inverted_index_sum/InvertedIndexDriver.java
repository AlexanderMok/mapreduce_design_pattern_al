package chp2_summarization_pattern.inverted_index_sum;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import utils.MPRutil;

import org.apache.commons.lang.StringEscapeUtils;

/**
 * The breakdown of each MapReduce component is described in detail below:
 * <ul>
 * <li>The mapper outputs the desired fields for the index as the key and the
 * unique identifier as the value.</li>
 * <li>The combiner can be omitted if you are just using the identity reducer,
 * because under those circumstances a combiner would just create unnecessary
 * processing. Some implementations concatenate the values associated with a
 * group before outputting them to the file system. In this case, a combiner can
 * be used. It won’t have as beneficial an impact on byte count as the combiners
 * in other patterns, but there will be an improvement.</li>
 * <li>The partitioner is responsible for determining where values with the same
 * key will eventually be copied by a reducer for final output. It can be
 * customized for more efficient load balancing if the intermediate keys are not
 * evenly distributed.</li>
 * <li>The reducer will receive a set of unique record identifiers to map back
 * to the input key. The identifiers can either be concatenated by some unique
 * delimiter, leading to the output of one key/value pair per group, or each
 * input value can be written with the input key, known as the identity reducer.
 * </li>
 * 
 * @author Alex
 *
 */
public class InvertedIndexDriver
{
	public static String getWikipediaURL(String text)
	{

		int idx = text.indexOf("\"http://en.wikipedia.org");
		if (idx == -1)
		{
			return null;
		}
		int idx_end = text.indexOf('"', idx + 1);

		if (idx_end == -1)
		{
			return null;
		}

		int idx_hash = text.indexOf('#', idx + 1);

		if (idx_hash != -1 && idx_hash < idx_end)
		{
			return text.substring(idx + 1, idx_hash);
		} else
		{
			return text.substring(idx + 1, idx_end);
		}

	}

	public static class WikipediaExtractor extends Mapper<Object, Text, Text, Text>
	{

		private Text link = new Text();
		private Text outkey = new Text();
        
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{

			// Parse the input string into a nice map
			Map<String, String> parsed = MPRutil.transformXmlToMap(value.toString());

			// Grab the necessary XML attributes
			String txt = parsed.get("Body");
			String posttype = parsed.get("PostTypeId");
			String row_id = parsed.get("Id");

			// if the body is null, or the post is a question (1), skip
			if (txt == null || (posttype != null && posttype.equals("1")))
			{
				return;
			}

			// Unescape the HTML because the SO data is escaped.
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

			link.set(getWikipediaURL(txt));
			outkey.set(row_id);
			context.write(link, outkey);
		}
	}

	public static class Concatenator extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();
        
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{

			StringBuilder sb = new StringBuilder();
			for (Text id : values)
			{
				sb.append(id.toString() + " ");
			}

			result.set(sb.substring(0, sb.length() - 1).toString());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: WikipediallIndex <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "StackOverflow Wikipedia URL Inverted Index");
		
		job.setJarByClass(InvertedIndexDriver.class);
		
		job.setMapperClass(WikipediaExtractor.class);
		job.setCombinerClass(Concatenator.class);
		job.setReducerClass(Concatenator.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
