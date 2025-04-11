import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonWordCount 
{
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			// convert text to a single case and exclude other punctuation from the text 
			
			StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^a-z0-9']", " "));
			while (itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private IntWritable result = new IntWritable();
		private Map<Text, IntWritable> commonMap = new HashMap<>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			commonMap.put(new Text(key), new IntWritable(sum));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			Map<Text, IntWritable> sortCommonMap = sortMapByValues(commonMap, Comparator.comparingInt(IntWritable::get).reversed());
			int sum = 0;
			for(Text key: sortCommonMap.keySet())
			{
				if(sum++ == 25)
				{
					break;
				}
				context.write(key, sortCommonMap.get(key));
			}
			
		}
		
		public static <K, V> Map<K, V> sortMapByValues(Map<K, V> map, Comparator<? super V> comparator) 
		{
            return map.entrySet()
                    .stream()
                    .sorted(Map.Entry.<K, V>comparingByValue(comparator))
                    .collect(LinkedHashMap::new, (m, c) -> m.put(c.getKey(), c.getValue()), Map::putAll);
        }
	}
	

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		
		// Get count of all words
		Job job = Job.getInstance(conf, "common words");
		job.setJarByClass(CommonWordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
