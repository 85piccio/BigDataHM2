package it.uniroma1.bdc.hm2.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class App {

	static int printUsage() {
		System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static void main(String[] args) throws Exception {
		
		List<String> otherArgs = new ArrayList<String>();

		Configuration conf = new Configuration();
		
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setInt("mapreduce.job.maps", Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setInt("mapreduce.job.reduces", Integer.parseInt(args[++i]));
				} else {
					otherArgs.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				System.exit(printUsage());
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				System.exit(printUsage());
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (otherArgs.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					otherArgs.size() + " instead of 2.");
			System.exit(printUsage());
		}
		
		Path input = new Path(otherArgs.get(0));
		Path output =new Path(otherArgs.get(1));
		
		Job job = Job.getInstance(conf);
	    FileInputFormat.setInputPaths(job, input);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setMapperClass(MyMapper.class);

	    FileOutputFormat.setOutputPath(job, output);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    job.setCombinerClass(MyReducer.class);
	    job.setReducerClass(MyReducer.class);

	    job.waitForCompletion(true);

	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
		}

		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.run(context);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(key, values, context);
		}
		@Override
		public void run(Context arg0) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.run(arg0);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
	}
}