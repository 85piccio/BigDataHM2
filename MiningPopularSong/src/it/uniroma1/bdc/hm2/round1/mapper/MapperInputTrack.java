package it.uniroma1.bdc.hm2.round1.mapper;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperInputTrack extends Mapper<LongWritable, Text, Text, Text> {

	// private final static IntWritable one = new IntWritable(1);
	// private Text word = new Text();

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// TODO Auto-generated method stub
		// super.map(key, value, context);

		Scanner scanner = new Scanner(value.toString());
		scanner.useDelimiter("\n");

		while (scanner.hasNext()) {
			String[] parts = scanner.next().split("\t");
			if (!(parts[3].isEmpty() || parts[5].isEmpty()) )
				context.write(new Text(parts[0]/* UID */), new Text(parts[3]+"$"+parts[5]/* idtrack */));/* emit */
		}
		scanner.close();
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

}
