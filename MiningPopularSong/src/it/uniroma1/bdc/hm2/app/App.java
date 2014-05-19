package it.uniroma1.bdc.hm2.app;


//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Scanner;

import it.uniroma1.bdc.hm2.round1.mapper.MapperInputCountry;
import it.uniroma1.bdc.hm2.round1.mapper.MapperInputTrack;
import it.uniroma1.bdc.hm2.round1.reducer.ReducerJoint;

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

		Path input1 = new Path("/in/input1.txt");// country
		Path input2 = new Path("/in/input2.txt");// truck

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

}
