package sol3;
import java.io.IOException;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class Sol3 {
	
	public static class TokenizerMapper extends Mapper<Object, Text, NewText, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private NewText word = new NewText();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.setWord(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<NewText, IntWritable, NewText, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(NewText key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String... args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		new Path(args[1]).getFileSystem(conf).delete(new Path(otherArgs[1]), true);

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Sol3.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(NewText.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("\n***************** " + (result ? "Job 3 SUCCESS" : "Job 3 FAILED") + "\n");
		if(!result)
			System.exit(1);
	}
}