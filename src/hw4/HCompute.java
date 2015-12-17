package hw4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This class reads from the HBase table to generate the desired output file.
 */
public class HCompute {

	private static final String TARGET_YEARSTART = "2008";
	private static final String TARGET_YEAREND = "2009";
	public static final String TABLE_NAME = "FlightData";
	public static final String COLUMN_FAMILY = "FlightInfo";
	public static final String COLUMN_ARRDELAYMINUTES = "ArrDelayMinutes";

	public static class HComputeMapper extends TableMapper<KeyPair, Text> {

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			KeyPair key = new KeyPair();
			Text arrDelayMinutes = new Text();

			// Parse the rowKey to an array of String
			// [Year,Month,DayofMonth,UniqueCarrier,FlightNum,Origin]
			String[] rowKey = new String(value.getRow()).split(",");

			// Set key as (AirlineName, Month) KeyPair
			key.setAirlineName(rowKey[3]);
			key.setMonth(rowKey[1]);

			// Set value as arrDelayMinutes
			arrDelayMinutes.set(new String(
					value.getValue(COLUMN_FAMILY.getBytes(),
							COLUMN_ARRDELAYMINUTES.getBytes())));

			context.write(key, arrDelayMinutes);
		}
	}

	public static class HComputeReducer extends
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
					int avgDelay = (int) Math.ceil(sumDelay
							/ (double) sumFlights);
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

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: H-COMPUTE <out>");
			System.exit(2);
		}

		// Set startRow and stopRow to do range search
		// Only scan the regions which contains target year
		Scan scan = new Scan(TARGET_YEARSTART.getBytes(),
				TARGET_YEAREND.getBytes());
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		Job job = new Job(conf, "H-COMPUTE");
		job.setJarByClass(HCompute.class);
		job.setMapperClass(HComputeMapper.class);
		job.setReducerClass(HComputeReducer.class);
		job.setOutputKeyClass(KeyPair.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan,
				HComputeMapper.class, KeyPair.class, Text.class, job);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
