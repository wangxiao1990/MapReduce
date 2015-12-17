package hw4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * The class KeyPair consists of airlineName and month. It implements the
 * WritableComparable interface and defines necessary methods used for
 * KeyComparator and GroupComparator
 */
@SuppressWarnings("rawtypes")
public class KeyPair implements WritableComparable {

	private Text airlineName;
	private Text month;

	public KeyPair() {
		this.airlineName = new Text();
		this.month = new Text();
	}

	public KeyPair(Text airlineName, Text month) {
		this.airlineName = airlineName;
		this.month = month;
	}

	public Text getAirlineName() {
		return this.airlineName;
	}

	public void setAirlineName(String airlineName) {
		this.airlineName.set(airlineName.getBytes());
	}

	public Text getMonth() {
		return this.month;
	}

	public void setMonth(String month) {
		this.month.set(month.getBytes());
	}

	/**
	 * Override the write method
	 */
	public void write(DataOutput out) throws IOException {
		this.airlineName.write(out);
		this.month.write(out);
	}

	/**
	 * Override the readFiled method
	 */
	public void readFields(DataInput in) throws IOException {
		if (this.airlineName == null)
			this.airlineName = new Text();

		if (this.month == null)
			this.month = new Text();

		this.airlineName.readFields(in);
		this.month.readFields(in);
	}

	/**
	 * Override compareTo method for KeyComparator Sort airlineName in
	 * alphabetic order first and then sort month in increasing order
	 */
	public int compareTo(Object o) {
		KeyPair kp = (KeyPair) o;
		if (this.airlineName.equals(kp.getAirlineName())) {
			Integer month_self = Integer.parseInt(this.month.toString());
			Integer month_kp = Integer.parseInt(kp.getMonth().toString());
			return month_self.compareTo(month_kp);
		}
		return this.airlineName.compareTo(kp.getAirlineName());
	}

	/**
	 * Provide comparator for GroupComparator which only sorts airlineName in
	 * alphabetic order regardless of month
	 */
	public int compare(Object o) {
		KeyPair kp = (KeyPair) o;
		return this.airlineName.compareTo(kp.getAirlineName());
	}

	/**
	 * Use hashcode of airlineName for partitioning
	 */
	public int hashCode() {
		return this.airlineName.hashCode();
	}
}
