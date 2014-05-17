package it.uniroma1.bdc.hm2.app;

import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class App {

	static int printUsage() {
		System.out
				.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static void main(String[] args) throws Exception {

		// List<String> otherArgs = new ArrayList<String>();

		Configuration conf = new Configuration();
		//
		// for(int i=0; i < args.length; ++i) {
		// try {
		// if ("-m".equals(args[i])) {
		conf.setInt("mapreduce.job.maps", 4);
		// } else if ("-r".equals(args[i])) {
		conf.setInt("mapreduce.job.reduces", 4);
		// } else {
		// otherArgs.add(args[i]);
		// }
		// } catch (NumberFormatException except) {
		// System.out.println("ERROR: Integer expected instead of " + args[i]);
		// System.exit(printUsage());
		// } catch (ArrayIndexOutOfBoundsException except) {
		// System.out.println("ERROR: Required parameter missing from " +
		// args[i-1]);
		// System.exit(printUsage());
		// }
		// }
		// // Make sure there are exactly 2 parameters left.
		// if (otherArgs.size() != 2) {
		// System.out.println("ERROR: Wrong number of parameters: " +
		// otherArgs.size() + " instead of 2.");
		// System.exit(printUsage());
		// }

		Path input1 = new Path("/in/input1.txt");
		Path input2 = new Path("/in/input2.txt");
		// Path input2 = new
		// Path("/in/userid-timestamp-artid-artname-traid-traname.tsv");

		Path output = new Path("/out");

		Job job = Job.getInstance(conf);
		job.setJarByClass(App.class);
		
		MultipleInputs.addInputPath(job, input1, TextInputFormat.class, MyMapper.class);
        MultipleInputs.addInputPath(job,input2, TextInputFormat.class, MyMapper2.class);

//		FileInputFormat.setInputPaths(job, input1);
//		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);

		FileOutputFormat.setOutputPath(job, output);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);

	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		// private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();

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
			// super.map(key, value, context);

			Scanner scanner = new Scanner(value.toString());
			scanner.useDelimiter("\n");

			while (scanner.hasNext()) {
				String[] parts = scanner.next().split("\t");

				if (parts.length > 2) {
					if (parts[3].compareTo("") != 0)//ignore uid without country 
						context.write(new Text(parts[0]/* UID */), new Text(parts[3]/* Country */));/* emit */
				}
			}
			scanner.close();
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
	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text>{

		// private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();

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
			// super.map(key, value, context);

			Scanner scanner = new Scanner(value.toString());
			scanner.useDelimiter("\n");

			while (scanner.hasNext()) {
				String[] parts = scanner.next().split("\t");

				if (parts.length > 2) {
					if (parts[3].compareTo("") != 0)//ignore uid without country 
						context.write(new Text(parts[0]/* UID */), new Text(parts[3]/* Country */));/* emit */
				}
			}
			scanner.close();
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

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text value : values)
				context.write(key, value);
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