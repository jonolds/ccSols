package sol4;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class relativefreq2 {
	
	public static class PairsRelativeOccurrenceMapper extends Mapper<LongWritable, Text, wordpair2, IntWritable> {
		private wordpair2 wordPair = new wordpair2();
		private IntWritable ONE = new IntWritable(1), totalCount = new IntWritable();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int neighbors = context.getConfiguration().getInt("neighbors", 1);
			String[] tokens = value.toString().split("\\s+");
			if (tokens.length > 1) {
				for (int i = 0; i < tokens.length; i++) {
					tokens[i] = tokens[i].replaceAll("\\W+","");

					if(tokens[i].equals(""))
						continue;
					wordPair.setWord(tokens[i]);
					int start = (i - neighbors < 0) ? 0 : i - neighbors;
					int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
					for (int j = start; j <= end; j++) {
						if (j == i) continue;
						wordPair.setNeighbor(tokens[j].replaceAll("\\W",""));
						context.write(wordPair, ONE);
					}
					wordPair.setNeighbor("*");
					totalCount.set(end - start);
					context.write(wordPair, totalCount);
				}
			}
		}
	}

	public	static class PairsReducer extends Reducer<wordpair2,IntWritable,wordpair2,IntWritable> {
		private IntWritable totalCount = new IntWritable();
		
		protected void reduce(wordpair2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values)
				count += value.get();
			totalCount.set(count);
			context.write(key,totalCount);
		}
	}

	public static class PairsRelativeOccurrenceReducer extends Reducer<wordpair2, IntWritable, wordpair2, DoubleWritable> {
		private DoubleWritable totalCount = new DoubleWritable(), relativeCount = new DoubleWritable();
		private Text currentWord = new Text("NOT_SET"), flag = new Text("*");

		protected void reduce(wordpair2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			if (key.getNeighbor().equals(flag)) {
				if (key.getWord().equals(currentWord))
					totalCount.set(totalCount.get() + getTotalCount(values));
				else {
					currentWord.set(key.getWord());
					totalCount.set(0);
					totalCount.set(getTotalCount(values));
				}
			}
			else {
				int count = getTotalCount(values);
				relativeCount.set((double) count / totalCount.get());
				context.write(key, relativeCount);
			}
		}
		private int getTotalCount(Iterable<IntWritable> values) {
			int count = 0;
			for (IntWritable value : values)
				count += value.get();
			return count;
		}
	}
	
	public static void imp2main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: relativefreq2 <in> <out>");
			System.exit(1);
		}
		new Path(args[1]).getFileSystem(conf).delete(new Path(otherArgs[1]), true);
		
		Job job = Job.getInstance(conf, "relativefreq2 Sol4");
		job.setJarByClass(relativefreq2.class);
		job.setMapperClass(PairsRelativeOccurrenceMapper.class); 
		job.setReducerClass(PairsRelativeOccurrenceReducer.class);
		job.setCombinerClass(PairsReducer.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(wordpair2.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/out2"));

		boolean result = job.waitForCompletion(true);
		System.out.println("\n***************** " + (result ? "Job 4 SUCCESS" : "Job 4 FAILED") + "\n");
		if(!result)
			System.exit(1);
	}
}