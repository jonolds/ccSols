package sol5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

public class wordpair implements Writable, WritableComparable<wordpair> {
	private Text word, neighbor;

	public wordpair(Text word, Text neighbor) {
		this.word = word;
		this.neighbor = neighbor;
	}

	public wordpair(String word, String neighbor) {
		this(new Text(word),new Text(neighbor));
	}

	public wordpair() {
		this.word = new Text();
		this.neighbor = new Text();
	}

	public int compareTo(wordpair other) {
		int returnVal = this.word.compareTo(other.getWord());
		if(returnVal != 0)
			return returnVal;
		if(this.neighbor.toString().equals("*"))
			return -1;
		else if(other.getNeighbor().toString().equals("*"))
			return 1;
		return this.neighbor.compareTo(other.getNeighbor());
	}

	public static wordpair read(DataInput in) throws IOException {
		wordpair wordPair = new wordpair();
		wordPair.readFields(in);
		return wordPair;
	}

	public void write(DataOutput out) throws IOException {
		word.write(out);
		neighbor.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		neighbor.readFields(in);
	}

	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		wordpair wordPair = (wordpair) o;
		if (neighbor != null ? !neighbor.equals(wordPair.neighbor) : wordPair.neighbor != null) return false;
		if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;
		return true;
	}

	public int hashCode() { return word != null ? word.hashCode() : 0; }
	public void setWord(String word){ this.word.set(word); }
	public void setNeighbor(String neighbor){ this.neighbor.set(neighbor); }
	public Text getWord() { return word; }
	public Text getNeighbor() { return neighbor; }
	
	public static class WordPairPartitioner extends Partitioner<wordpair,IntWritable> {
		public int getPartition(wordpair wordPair, IntWritable intWritable, int numPartitions) {
			return (Integer.MAX_VALUE & wordPair.getWord().hashCode()) % numPartitions;
		}
	}
}