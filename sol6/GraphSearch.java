package sol6;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GraphSearch extends Configured implements Tool {
	/**Nodes that are WHITE or BLACK are emitted, as is. For every edge of a Color.GRAY node, we 
	emit a new Node with distance + 1. The Color.GRAY node is then colored black and is also emitted. */
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			Node node = new Node(value.toString());
			Iterator<Integer> w = node.getWeights().iterator();
			int ww = 0;		
			if (node.getColor() == Node.Color.GRAY) {
				for (int v : node.getEdges()) {
					Node vnode = new Node(v);
					if(w.hasNext())
						ww = (int)w.next();
					vnode.setDistance(node.getDistance() + ww);
					vnode.setColor(Node.Color.GRAY);
					output.collect(new IntWritable(vnode.getId()), vnode.getLine());
				}
				node.setColor(Node.Color.BLACK);
			}
			output.collect(new IntWritable(node.getId()), node.getLine());
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		/** Build composite Node. It should have: full list of edges - min distance - The darkest Color */
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException { 	
			List<Integer> edges = null, weights = null;	
			int distance = Integer.MAX_VALUE, currentDistance = -1;
			Node.Color color = Node.Color.WHITE;

			while(values.hasNext()) {
				Text value = values.next();
				Node u = new Node(key.get() + "\t" + value.toString());
				if(u.getColor() == Node.Color.BLACK)
					currentDistance = u.getDistance();
				edges = (u.getEdges().size() > 0) ? u.getEdges() : edges;
				weights = (u.getWeights().size() > 0) ? u.getWeights() : weights;
				// Save the minimum distance and the darkest color
				distance = (u.getDistance() < distance) ? u.getDistance() : distance;
				color = (u.getColor().ordinal() > color.ordinal()) ? u.getColor() : color;
			} 
			if(distance < currentDistance)
				color = Node.Color.GRAY;	 	
			output.collect(key, new Text(new Node(key.get(), distance, edges, weights, color).getLine()));		
		}
	}

	private JobConf getJobConf(String[] args) {
		JobConf conf = new JobConf(getConf(), GraphSearch.class);
		conf.setJobName("graphsearch");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		for (int i = 0; i < args.length; ++i) {
			if ("-m".equals(args[i]))
				conf.setNumMapTasks(Integer.parseInt(args[++i]));
			else if ("-r".equals(args[i]))
				conf.setNumReduceTasks(Integer.parseInt(args[++i]));
		}
		return conf;
	}

	/** The main driver for word count map/reduce program. Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the job tracker. */
	public int run(String[] args) throws Exception {
		int totalCount=4;
		for (int i = 0; i < args.length; ++i)
			if ("-i".equals(args[i]))
				totalCount= Integer.parseInt(args[++i]);
		JobConf conf = getJobConf(args);
		new Path(args[1]).getFileSystem(conf).delete(new Path("sol6/output"), true); //DELETE OUTPUT
		for(int iter = 0; iter < totalCount; iter++) {
			String input = (iter == 0) ? "sol6/input-graph" : "sol6/output/output-graph-" + (iter);
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path("sol6/output/output-graph-" + (iter + 1)));
			JobClient.runJob(conf);
		}
		return 0;
	}

	public static void main6(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GraphSearch(), args);
		System.exit(res);
	}
}