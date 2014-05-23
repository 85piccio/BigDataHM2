package it.uniroma1.bdc.hm2.round2.reducer;

import java.util.Comparator;

public class Song implements Comparable<Song> {
	private String name;
	private Integer nPlayed;

	public Song(String name, Integer nPlayed) {
		this.name = name;
		this.nPlayed = nPlayed;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getnPlayed() {
		return nPlayed;
	}

	public void setnPlayed(Integer nPlayed) {
		this.nPlayed = nPlayed;
	}

	@Override
	public int compareTo(Song o) {
		return this.getnPlayed() - o.getnPlayed();
	}

	public static Comparator<Song> SongComparator = new Comparator<Song>() {

		public int compare(Song s1, Song s2) {
			return s1.compareTo(s2);
		}

	};
	
	//comparator for reversing	
	public static Comparator<Song> ReverseSongComparator = new Comparator<Song>() {

		public int compare(Song s1, Song s2) {
			return -s1.compareTo(s2);
		}

	};
}