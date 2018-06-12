package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Test_ChainMapper {
	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");
		
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		boolean success = job1.waitForCompletion(true);
		
		if (success)
			System.exit(1);
		else
			System.exit(0);
	}
}
