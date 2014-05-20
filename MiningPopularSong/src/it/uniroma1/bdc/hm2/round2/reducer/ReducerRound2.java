package it.uniroma1.bdc.hm2.round2.reducer;



import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
		Integer userCount = 0;
		Map<String,Integer> userCheck = new HashMap<>();
		for (Text value : values) {
			String data[] = value.toString().split("\t");
			
			if(userCheck.containsKey(data[0]))
				userCount++;
			else
				userCheck.put(data[0],1);
		}
		context.write(new Text(key.toString().substring(2)), new Text(userCount.toString()));

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
