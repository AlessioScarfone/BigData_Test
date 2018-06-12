package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigDataTest_Ex1 {

	public static class KeySolverTime implements WritableComparable<KeySolverTime> {
		String solver;
		String time;

		public KeySolverTime() {
		}

		public KeySolverTime(String solver, String time) {
			super();
			this.solver = solver;
			this.time = time;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			solver = WritableUtils.readString(in);
			time = WritableUtils.readString(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, solver);
			WritableUtils.writeString(out, time);
		}

		@Override
		public int compareTo(KeySolverTime o) {
			int cmp = solver.compareTo(o.solver);
			if (cmp == 0) {
				Double d1 = Double.parseDouble(time);
				Double d2 = Double.parseDouble(o.time);
				cmp = d1.compareTo(d2);
			}
			return cmp;
		}

		@Override
		public String toString() {
			return solver + "\t" + time;
		}
	}

	public static class KeySolverTimeComparator extends WritableComparator {

		protected KeySolverTimeComparator() {
			super(KeySolverTime.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			KeySolverTime ac = (KeySolverTime) a;
			KeySolverTime bc = (KeySolverTime) b;
			return ac.compareTo(bc);
		}
	}

	public static class KeySolverTimeGroupComparator extends WritableComparator {

		protected KeySolverTimeGroupComparator() {
			super(KeySolverTime.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			KeySolverTime ac = (KeySolverTime) a;
			KeySolverTime bc = (KeySolverTime) b;
			if (ac.solver.equals(bc.solver))
				return 0;
			return 1;
		}
	}

	/**
	 *  split input and write in output my custom key KeySolverTime(composed by solver and time)  
	 *	and use as value the time
	 */
	public static class Mapper1 extends Mapper<LongWritable, Text, KeySolverTime, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, KeySolverTime, Text>.Context context)
				throws IOException, InterruptedException {

			String[] tok = value.toString().split("\t");
			if (tok[14].equals("solved")) {
				KeySolverTime k = new KeySolverTime(tok[0], tok[11]);
				context.write(k, new Text(tok[11]));
			}
		}
	}

	/**
	 * Sort for time and group for solver name. 
	 * The resolver write in output solver and the list of its times 
	 */
	public static class Reducer1 extends Reducer<KeySolverTime, Text, Text, Text> {

		@Override
		protected void reduce(KeySolverTime key, Iterable<Text> values,
				Reducer<KeySolverTime, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			String s = "";
			for (Text text : values) {
				s += text + "\t";
			}
			context.write(new Text(key.solver), new Text(s));
		}
	}

	/**
	 * Read the output of previous Reducer and split input and add an index to each time.
	 * Write in output index -> solver,time
	 */
	public static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {

			String[] tok = value.toString().split("\t");
			String solver = tok[0];
			for (int i = 1; i < tok.length; i++) {
				context.write(new IntWritable(i), new Text(solver + "\t" + tok[i]));
			}
		}
	}

	/**
	 * Data are grouping for index 
	 * ex: 0 -> solver1 t_solver1 solver2 t_solver2 ...
	 * 	   1 -> solver2 t_solver2 solver1 t_solver1 ...
	 * Fixed an order of the solvers for print, we can create a csv file for the output 
	 */
	public static class Reducer2 extends Reducer<IntWritable, Text, NullWritable, Text> {
		private boolean firstit = true;
		private Map<String, String> solverMap = new HashMap<>();
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			
			for (Text text : values) {
				String[] tok = text.toString().split("\t");
				String solver = tok[0];
				String time = tok[1];
				solverMap.put(solver, time);
			}
			if(firstit) {
				firstit=false;
				String it = "N_inst, ";
				for (String solvKey : solverMap.keySet()) {
					it+=solvKey+", ";
				}
				
				context.write(NullWritable.get(), new Text(it.substring(0, it.length() - 2)));
			}
			
			String timeVal = key.get()+", ";
			for (String solvKey : solverMap.keySet()) {
				timeVal+=solverMap.get(solvKey)+", ";
			}
			context.write(NullWritable.get(), new Text(timeVal.substring(0, timeVal.length() - 2)));
			
			for (String solvKey : solverMap.keySet()) {
				solverMap.put(solvKey, "----");
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");

		job1.setMapperClass(Mapper1.class);
		job1.setSortComparatorClass(KeySolverTimeComparator.class);
		job1.setGroupingComparatorClass(KeySolverTimeGroupComparator.class);

		job1.setReducerClass(Reducer1.class);

		job1.setOutputKeyClass(KeySolverTime.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		boolean success = job1.waitForCompletion(true);
		if (success) {
			Job job2 = Job.getInstance(conf, "Job2");

			job2.setMapperClass(Mapper2.class);

			job2.setReducerClass(Reducer2.class);

			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/1"));

			success = job2.waitForCompletion(true);
		}

		if (success)
			System.exit(0);
		else
			System.exit(1);
	}
}
