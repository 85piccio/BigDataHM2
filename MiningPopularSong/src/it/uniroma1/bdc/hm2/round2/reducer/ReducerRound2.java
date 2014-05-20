package it.uniroma1.bdc.hm2.round2.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerRound2 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Integer userCount = 0;
		Map<String, Integer> userCheck = new HashMap<>();
		Map<String, Integer> totSong = new HashMap<>();

		for (Text value : values) {
			String data[] = value.toString().split("\t");

			// conta utenti
			if (userCheck.get(data[0]) != 1)
				userCount++;
			else
				userCheck.put(data[0], 1);

			// conta totali canzoni
			Integer incr = new Integer(data[2]);

			if (totSong.containsKey(data[1]))
				totSong.put(data[1], totSong.get(data[1]) + incr);// incr
																	// counter
			else
				totSong.put(data[1], incr);// init counter

		}
		Iterator<Entry<String, Integer>> iterator = totSong.entrySet().iterator();
		// String s = "";
		key = new Text(key.toString().substring(2));
		String sBest="";
		Integer Best = 0;
		while (iterator.hasNext()) {// for each track played in a country
			
			Entry<String, Integer> song = iterator.next();
			
			if(Best < song.getValue()){
				sBest =  song.getKey() + "\t" + song.getValue();
				Best = song.getValue();
			}
//			context.write(key, new Text(userCount + "\t" + song.getKey() + "\t" + song.getValue()));
			iterator.remove();
		}
		context.write(key, new Text(userCount + "\t" + sBest));
		// reset key ( -$$ from country )
//		key = new Text(key.toString().substring(2));
//		context.write(key, new Text(userCount.toString() + "\t" + totSong.size()));

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
