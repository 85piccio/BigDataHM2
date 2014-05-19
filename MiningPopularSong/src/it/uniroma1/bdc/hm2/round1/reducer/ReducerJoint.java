package it.uniroma1.bdc.hm2.round1.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoint extends Reducer<Text, Text, Text, Text> {

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
		String country = "";
		Map<String, Integer> songCount = new HashMap<>();
		for (Text value : values) {
			if (value.toString().contains("#")/* regex coountry */) {// value
																	// =
																	// country
																	// TODO:
																	// PERFORMANCE
				country = value.toString();
			} else {// Value = id track
				if (songCount.containsKey(value.toString())) {
					songCount.put(value.toString(),
							songCount.get(value.toString()) + 1);
				} else {
					songCount.put(value.toString(), 1);
				}
			}
		}
		if (country.compareTo("") != 0 && songCount.size() > 0) {// fix no
																	// country
																	// set
			Iterator<Entry<String, Integer>> iterator = songCount
					.entrySet().iterator();
			String s = "";
			while (iterator.hasNext()) {// for each track played
				Entry<String, Integer> song = iterator.next();
				s += song.getKey() + "\t" + song.getValue() + "\t";
				iterator.remove();
			}
			context.write(key, new Text(country + "\t" + s));
		}

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
