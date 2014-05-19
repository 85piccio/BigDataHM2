package it.uniroma1.bdc.hm2.app;

import it.uniroma1.bdc.hm2.mapper.MapperInputCountry;
import it.uniroma1.bdc.hm2.mapper.MapperInputTrack;
import it.uniroma1.bdc.hm2.reducer.ReducerJoint;

//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
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
		conf.setInt("mapreduce.job.maps", 1);
		// } else if ("-r".equals(args[i])) {
		conf.setInt("mapreduce.job.reduces", 1);
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

		Path input1 = new Path("/in/input1.txt");//country
		Path input2 = new Path("/in/input2.txt");//truck

		Path output = new Path("/out");

		Job job = Job.getInstance(conf);
		job.setJarByClass(App.class);

		MultipleInputs.addInputPath(job, input1, TextInputFormat.class,
				MapperInputCountry.class);
		MultipleInputs.addInputPath(job, input2, TextInputFormat.class,
				MapperInputTrack.class);

		// FileInputFormat.setInputPaths(job, input1);
		// job.setInputFormatClass(TextInputFormat.class);
		// job.setMapperClass(MyMapper.class);

		FileOutputFormat.setOutputPath(job, output);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(ReducerJoint.class);

		job.waitForCompletion(true);

	}

//	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//		// private final static IntWritable one = new IntWritable(1);
//		// private Text word = new Text();
//
//		@Override
//		protected void cleanup(Context context) throws IOException,
//				InterruptedException {
//			// TODO Auto-generated method stub
//			super.cleanup(context);
//		}
//
//		@Override
//		protected void map(LongWritable key, Text value, Context context)
//				throws IOException, InterruptedException {
//
//			// TODO Auto-generated method stub
//			// super.map(key, value, context);
//
//			Scanner scanner = new Scanner(value.toString());
//			scanner.useDelimiter("\n");
//
//			while (scanner.hasNext()) {
//				String[] parts = scanner.next().split("\t");
//
//				if (parts.length > 3) {
//					if (!parts[3].isEmpty()) {// ignore uid without
//												// country
//
//						context.write(new Text(parts[0]/* UID */), new Text(
//								"#" + parts[3]/* Country */));/* emit */
//					}
//				}
//			}
//			scanner.close();
//		}
//
//		@Override
//		public void run(Context context) throws IOException,
//				InterruptedException {
//			// TODO Auto-generated method stub
//			super.run(context);
//		}
//
//		@Override
//		protected void setup(Context context) throws IOException,
//				InterruptedException {
//			// TODO Auto-generated method stub
//			super.setup(context);
//		}
//
//	}

//	public static class MyMapper2 extends
//			Mapper<LongWritable, Text, Text, Text> {
//
//		// private final static IntWritable one = new IntWritable(1);
//		// private Text word = new Text();
//
//		@Override
//		protected void cleanup(Context context) throws IOException,
//				InterruptedException {
//			// TODO Auto-generated method stub
//			super.cleanup(context);
//		}
//
//		@Override
//		protected void map(LongWritable key, Text value, Context context)
//				throws IOException, InterruptedException {
//
//			// TODO Auto-generated method stub
//			// super.map(key, value, context);
//
//			Scanner scanner = new Scanner(value.toString());
//			scanner.useDelimiter("\n");
//
//			while (scanner.hasNext()) {
//				String[] parts = scanner.next().split("\t");
//				// if (parts.length == 5) {
//				if (!parts[2].isEmpty())
//					context.write(new Text(parts[0]/* UID */), new Text(
//							parts[2]/* idtrack */));/* emit */
//				// }s
//			}
//			scanner.close();
//		}
//
//		@Override
//		public void run(Context context) throws IOException,
//				InterruptedException {
//			// TODO Auto-generated method stub
//			super.run(context);
//		}
//
//		@Override
//		protected void setup(Context context) throws IOException,
//				InterruptedException {
//			// TODO Auto-generated method stub
//			super.setup(context);
//		}
//
//	}

//	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
//
//		@Override
//		protected void cleanup(Context context) throws IOException,
//				InterruptedException {
//			// TODO Auto-generated method stub
//			super.cleanup(context);
//		}
//
//		@Override
//		protected void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			String country = "";
//			Map<String, Integer> songCount = new HashMap<>();
//			for (Text value : values) {
//				if (value.toString().contains("#")/* regex coountry */) {// value
//																		// =
//																		// country
//																		// TODO:
//																		// PERFORMANCE
//					country = value.toString();
//				} else {// Value = id track
//					if (songCount.containsKey(value.toString())) {
//						songCount.put(value.toString(),
//								songCount.get(value.toString()) + 1);
//					} else {
//						songCount.put(value.toString(), 1);
//					}
//				}
//			}
//			if (country.compareTo("") != 0 && songCount.size() > 0) {// fix no
//																		// country
//																		// set
//				Iterator<Entry<String, Integer>> iterator = songCount
//						.entrySet().iterator();
//				String s = "";
//				while (iterator.hasNext()) {// for each track played
//					Entry<String, Integer> song = iterator.next();
//					s += song.getKey() + "\t" + song.getValue() + "\t";
//					iterator.remove();
//				}
//				context.write(key, new Text(country + "\t" + s));
//			}
//
//		}
//
//		@Override
//		public void run(Context arg0) throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			super.run(arg0);
//		}
//
//		@Override
//		protected void setup(Context context) throws IOException,
//				InterruptedException {
//			// TODO Auto-generated method stub
//			super.setup(context);
//		}
//	}
}
