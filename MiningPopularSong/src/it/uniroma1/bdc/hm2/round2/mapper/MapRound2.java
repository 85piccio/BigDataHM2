package it.uniroma1.bdc.hm2.round2.mapper;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapRound2 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// TODO Auto-generated method stub
		Scanner scanner = new Scanner(value.toString());
		scanner.useDelimiter("\n");

		while (scanner.hasNext()) {
			String[] parts = scanner.next().split("\t");
			// if (!(parts[3].isEmpty() || parts[5].isEmpty()))
			
			//PASS FORMAT -> "uid\tsong\tn_play"
			Text pass = new Text(parts[0]+"\t"+parts[2]+"\t"+parts[3]);
			context.write(new Text(parts[1]/* country */), pass );/* emit */
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
