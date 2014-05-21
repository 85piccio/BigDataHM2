package it.uniroma1.bdc.hm2.round2.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;

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
			if (!userCheck.containsKey(data[0])) {
				userCheck.put(data[0], 1);
				userCount++;
			}// else --> gia contato

			// conta totali canzoni
			Integer incr = new Integer(data[2]);

			if (totSong.containsKey(data[1]))
				// incr counter
				totSong.put(data[1], totSong.get(data[1]) + incr);
			else
				totSong.put(data[1], incr);// init counter

		}

		/*
		 * Best k element tramite min-binary-heap con size = k min sempre in
		 * radice -Se dim heap >= k - quando nextElem > root elimino root e
		 * inserisco nextElem nel heap - quando nextElem < root non è un best k
		 * --> scarto -altrimenti insert sempre
		 */

		Integer k = 5;
		Queue<Song> kbest = new PriorityQueue<>(k + 1, Song.SongComparator);

		Iterator<Entry<String, Integer>> iterator = totSong.entrySet().iterator();
		// String s = "";
		key = new Text(key.toString().substring(2));

		// insert first song
		Entry<String, Integer> firstSong = iterator.next();

		// inserisco prima canzone è stata suonata TODO: necessario?
		kbest.add(new Song(firstSong.getKey(), firstSong.getValue()));

		while (iterator.hasNext()) {// for each track played in a country

			Entry<String, Integer> song = iterator.next();

			if (kbest.size() < k) {
				kbest.add(new Song(song.getKey(), song.getValue()));
			} else if (song.getValue() > (kbest.peek().getnPlayed())) {
				// rimuovo ultimo elemento (il minore della k-selezione)
				kbest.remove();
				// add best
				kbest.add(new Song(song.getKey(), song.getValue()));
			}// Else --> scarto

			iterator.remove();
		}

		// //revert order
		// ArrayList<Song> a = new ArrayList<>();
		// a.addAll(kbest);
		// Collections.reverse(a);

		// for (Song s : a) {
		// context.write(key, new Text(userCount + "\t" + s.getName() + "\t" +
		// s.getnPlayed()));
		// }
		Queue<Song> reversekbest = new PriorityQueue<>(k, Song.ReverseSongComparator);
		while (!kbest.isEmpty()) {
			// Stampa kbest TODO:ordine inverso
			reversekbest.add(kbest.poll());
		}

		while (!reversekbest.isEmpty()) {
			Song best = reversekbest.poll();
			context.write(key, new Text(userCount + "\t" + best.getName() + "\t" + best.getnPlayed()));
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
