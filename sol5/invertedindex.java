package sol5;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class invertedindex {

	public static class WordcountMapper extends Mapper<LongWritable, Text, wordpair, IntWritable> {
		private Text filename = new Text();
		private wordpair wordPair = new wordpair();
		private Map<String, Integer> map;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, Integer> map = getMap();

			String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
			filename = new Text(filenameStr);
			int total = 0;

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken().toLowerCase();
				if (map.containsKey(token)) {
					total = map.get(token) + 1;
					map.put(token, total);

				}
				else {
					total = 1;
					map.put(token, total);
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Integer> map = getMap();
//			for (Map.Entry<String, Integer> entry : map.entrySet()) {
//			String key = entry.getKey().toString();
//			Integer value = entry.getValue();
//			System.out.println("key, " + key + " value " + value);
//		}
			Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Integer> entry = it.next();
				String sKey = entry.getKey();
				int total = entry.getValue().intValue();
				wordPair.setWord(sKey);
				wordPair.setNeighbor(filename.toString());
				context.write(wordPair, new IntWritable(total));
			}
		}

		public Map<String, Integer> getMap() {
			if (null == map)
				map = new HashMap<String, Integer>();
			return map;
		}

	}

	public static class WordcountReducer extends Reducer<wordpair, IntWritable, Text, Text> {
		private IntWritable result = new IntWritable();
		String prev = "";
		private Map<String, String> map;

		public void reduce(wordpair key, Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			Map<String, String> map = getMap();
			int sum = 0;

			for (IntWritable val : values) {
				if (!key.getWord().toString().equals(prev) && !prev.equals("")) {
					sum += val.get();
					result.set(sum);
					Map.Entry<String, String> entry = map.entrySet().iterator().next();
					String keyc = entry.getKey();
					String valuec = entry.getValue();
					context.write(new Text(keyc), new Text(valuec));
					map.clear();
				}
				
				String keys = map.get(key.getWord().toString()) + "" + key.getNeighbor().toString() + ":" + val.get();
				if (map.containsKey(key.getWord().toString()))
					map.put(key.getWord().toString(), keys);
				else
					map.put(key.getWord().toString(), key.getNeighbor().toString() + ": " + val.get());
				prev = key.getWord().toString();
			}
		}

		public Map<String, String> getMap() {
			if (null == map)
				map = new HashMap<String, String>();
			return map;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			Map.Entry<String, String> entry = map.entrySet().iterator().next();
			String keyc = entry.getKey();
			String valuec = entry.getValue();
			
			System.out.println(keyc + " val: " + valuec);
			context.write(new Text(keyc), new Text(valuec));
			map.clear();
		}
	}

	public static void main5(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		new Path(args[1]).getFileSystem(conf).delete(new Path(otherArgs[1]), true);
		Job job = Job.getInstance(conf, "Relative Frequency");

		job.setJarByClass(invertedindex.class);
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");

		job.setMapOutputKeyClass(wordpair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		job.setPartitionerClass(wordpair.WordPairPartitioner.class);
		job.setNumReduceTasks(3);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}