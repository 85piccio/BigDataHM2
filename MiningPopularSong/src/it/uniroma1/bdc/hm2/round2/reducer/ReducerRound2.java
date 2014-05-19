package it.uniroma1.bdc.hm2.round2.reducer;



import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerRound2  extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		for (Text value : values) {
			context.write(key, value);
		}

	}

	@Override
	public void run(Context arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(arg0);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

}
