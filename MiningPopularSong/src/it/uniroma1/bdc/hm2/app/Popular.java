package it.uniroma1.bdc.hm2.app;

import it.uniroma1.bdc.hm2.round1.mapper.MapperInputCountry;
import it.uniroma1.bdc.hm2.round1.mapper.MapperInputTrack;
import it.uniroma1.bdc.hm2.round1.reducer.ReducerJoint;
import it.uniroma1.bdc.hm2.round2.mapper.MapRound2;
import it.uniroma1.bdc.hm2.round2.reducer.ReducerRound2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Popular extends Configured implements Tool {

	static int printUsage() {
		System.out.println("popular.jar /LFM/users /LFM/plays /LFM/pop k \"country1[|country2]\"");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public final static String KBEST = "KBEST";
	public final static String COUNTRY = "COUNTRY";

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.maps", 5);

		conf.setInt("mapreduce.job.reduces", 5);
		
		Popular pop = new Popular();
		pop.setConf(conf);

		int res = ToolRunner.run(pop, args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Path input1 = new Path(args[0]);// User
		Path input2 = new Path(args[1]);// Play
		conf.setInt(KBEST, new Integer(args[2]));// kbest
		conf.setStrings(COUNTRY, args[3]); //valid country

		Path output1 = new Path("/result_job1");//temp results directory

		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Popular.class);

		MultipleInputs.addInputPath(job1, input1, TextInputFormat.class, MapperInputCountry.class);
		MultipleInputs.addInputPath(job1, input2, TextInputFormat.class, MapperInputTrack.class);

		FileOutputFormat.setOutputPath(job1, output1);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// job.setCombinerClass(MyReducer.class);
		job1.setReducerClass(ReducerJoint.class);

		job1.waitForCompletion(true);

		Path input3 = new Path("/result_job1/part*");

		// Formato file temp
		// id_utente\tcountry\ttitolo_autore$titolo_canzone\tn_suonate

		// Second round
		Path output2 = new Path("/pop");

		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(Popular.class);
		FileInputFormat.setInputPaths(job2, input3);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(MapRound2.class);

		FileOutputFormat.setOutputPath(job2, output2);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setReducerClass(ReducerRound2.class);

		job2.waitForCompletion(true);

		// Delete temp file
		FileSystem fs = FileSystem.get(conf);
		// delete file, true for recursive
		fs.delete(new Path("/result_job1/"), true);

		return job2.waitForCompletion(true) ? 0 : -1;
	}

}
