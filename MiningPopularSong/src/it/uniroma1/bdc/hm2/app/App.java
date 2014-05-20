package it.uniroma1.bdc.hm2.app;

import it.uniroma1.bdc.hm2.round1.mapper.MapperInputCountry;
import it.uniroma1.bdc.hm2.round1.mapper.MapperInputTrack;
import it.uniroma1.bdc.hm2.round1.reducer.ReducerJoint;
import it.uniroma1.bdc.hm2.round2.mapper.MapRound2;
import it.uniroma1.bdc.hm2.round2.reducer.ReducerRound2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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

		// List<String> otherArgs = new ArrayList<String>();

		Configuration conf = new Configuration();
		//
		// for(int i=0; i < args.length; ++i) {
		// try {
		// if ("-m".equals(args[i])) {
		conf.setInt("mapreduce.job.maps", 5);
		// } else if ("-r".equals(args[i])) {
		conf.setInt("mapreduce.job.reduces", 5);
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

		Path output1 = new Path("/temp");

		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(App.class);

		MultipleInputs.addInputPath(job1, input1, TextInputFormat.class, MapperInputCountry.class);
		MultipleInputs.addInputPath(job1, input2, TextInputFormat.class, MapperInputTrack.class);

		// FileInputFormat.setInputPaths(job, input1);
		// job.setInputFormatClass(TextInputFormat.class);
		// job.setMapperClass(MyMapper.class);

		FileOutputFormat.setOutputPath(job1, output1);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// job.setCombinerClass(MyReducer.class);
		job1.setReducerClass(ReducerJoint.class);

		job1.waitForCompletion(true);

		Path input3 = new Path("/temp/part*");// id_utente country
												// titolo_autore$titolo_canzone
												// n_suonate
		Path output2 = new Path("/out");

		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(App.class);
		FileInputFormat.setInputPaths(job2, input3);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(MapRound2.class);

		FileOutputFormat.setOutputPath(job2, output2);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setReducerClass(ReducerRound2.class);
		
		job2.waitForCompletion(true);
		
		//delate temp file
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/temp/"), true); // delete file, true for recursive 

	}

}
