package sol4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sol4Driver {

	public static void main4(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: relativefreq2 <in> <out>");
			System.exit(1);
		}
		new Path(args[1]).getFileSystem(conf).delete(new Path(otherArgs[1]), true);
		
		relativefreq1.imp1main(args);
		relativefreq2.imp2main(args);
	}
}