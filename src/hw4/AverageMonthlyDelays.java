package hw4;

import java.io.IOException;
import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageMonthlyDelays {

	private static final String TARGET_YEAR = "2008";
	private static final int YEAR_COLUMNINDEX = 0;
	private static final int MONTH_COLUMNINDEX = 2;
	private static final int UNIQUECARRIER_COLUMNINDEX = 6;
	private static final int DEPTIME_COLUMNINDEX = 24;
	private static final int ARRDELAYMINUTES_COLUMNINDEX = 37;

	public static class AverageMonthlyDelaysMapper extends
			Mapper<Object, Text, KeyPair, Text> {

		// CSVParser uses ',' as separator and '"' as char quote
		private CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {

			String[] record = this.csvParser.parseLine(value.toString());
			KeyPair key = new KeyPair();
			Text arrDelayMinutes = new Text();

			// Select only valid records
			if (record.length > 0 && isValidRecord(record)) {

				// Set key as (AirlineName, Month) KeyPair
				key.setAirlineName(record[UNIQUECARRIER_COLUMNINDEX]);
				key.setMonth(record[MONTH_COLUMNINDEX]);

				// Set value as arrDelayMinutes
				arrDelayMinutes.set(record[ARRDELAYMINUTES_COLUMNINDEX]);

				context.write(key, arrDelayMinutes);
			}
		}

		/**
		 * Validate the input record
		 */
		private boolean isValidRecord(String[] record) {

			if (record == null || record.length == 0) {
				return false;
			}

			// If any of the required attribute is missing, return false
			if (record[YEAR_COLUMNINDEX].isEmpty()
					|| record[MONTH_COLUMNINDEX].isEmpty()
					|| record[UNIQUECARRIER_COLUMNINDEX].isEmpty()
					|| record[DEPTIME_COLUMNINDEX].isEmpty()
					|| record[ARRDELAYMINUTES_COLUMNINDEX].isEmpty()) {
				return false;
			}

			// If the year of the flight does not match, return false
			if (!record[YEAR_COLUMNINDEX].equals(TARGET_YEAR)) {
				return false;
			}
			return true;
		}
	}

	public static class AverageMonthlyDelaysReducer extends
			Reducer<KeyPair, Text, Text, Text> {

		/**
		 * Each KeyPair with one specific airlineName will call reduce function
		 * once and months will be sorted in increasing order 
		 * key : KeyPair (airlineName, *) 
		 * values : delays sorted by month in increasing order for given key
		 */
		public void reduce(KeyPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sumDelay = 0;
			int sumFlights = 0;
			int currMonth = 1;
			StringBuilder output = new StringBuilder();

			// Write Airline Name first
			output.append(key.getAirlineName().toString());

			for (Text value : values) {

				// Check whether month changes for the value. If so, write
				// result to the output string and reset the counters
				if (currMonth != Integer.parseInt(key.getMonth().toString())) {
					int avgDelay = (int) Math.ceil(sumDelay / (double) sumFlights);
					output.append(", (" + currMonth + "," + avgDelay + ")");
					sumDelay = 0;
					sumFlights = 0;
					currMonth = Integer.parseInt(key.getMonth().toString());
				}

				sumDelay += Double.parseDouble(value.toString());
				sumFlights++;
			}

			// Write last result to the output string
			int avgDelay = (int) Math.ceil(sumDelay / (double) sumFlights);
			output.append(", (" + currMonth + "," + avgDelay + ")");

			context.write(new Text(), new Text(output.toString()));
		}
	}

	/**
	 * CustomPartitioner uses the hashcode of airlineName
	 */
	public static class CustomPartitioner extends Partitioner<KeyPair, Text> {
		/**
		 * According to the number of reducer, the key will be approximately
		 * evenly partitioned based on the amount of airline names
		 */
		@Override
		public int getPartition(KeyPair key, Text value, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}

	/**
	 * KeyComparator sort airlineName in alphabetic order first and then sort
	 * month in increasing order by using compareTo method of KeyPair class
	 */
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(KeyPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			KeyPair kp1 = (KeyPair) w1;
			KeyPair kp2 = (KeyPair) w2;
			return kp1.compareTo(kp2);
		}
	}

	/**
	 * GroupComparator only sorts airlineName in alphabetic order regardless of
	 * month by using compare method of KeyPair Class, so that one single reduce
	 * call would receive all KeyPairs for a given airlineName
	 */
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(KeyPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			KeyPair kp1 = (KeyPair) w1;
			KeyPair kp2 = (KeyPair) w2;
			return kp1.compare(kp2);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AverageMonthlyDelays <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Average monthly flight dealys");
		job.setJarByClass(AverageMonthlyDelays.class);
		job.setMapperClass(AverageMonthlyDelaysMapper.class);
		job.setReducerClass(AverageMonthlyDelaysReducer.class);
		job.setOutputKeyClass(KeyPair.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
