package hw3;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.opencsv.CSVParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageFlightDelay {

	private static final String ORIGIN = "ORD";
	private static final String DESTINATION = "JFK";
	private static final String PERIOD_STARTDATE = "2007-06-01";
	private static final String PERIOD_ENDDATE = "2008-05-31";
	private static final int FLIGHTDATE_COLUMNINDEX = 5;
	private static final int ORIGIN_COLUMNINDEX = 11;
	private static final int DEST_COLUMNINDEX = 17;
	private static final int DEPTIME_COLUMNINDEX = 24;
	private static final int ARRTIME_COLUMNINDEX = 35;
	private static final int ARRDELAYMINUTES_COLUMNINDEX = 37;
	private static final int CANCELLED_COLUMNINDEX = 41;
	private static final int DIVERTED_COLUMNINDEX = 43;

	/**
	 * The Mapper applies projections and selections to the input
	 */
	public static class AverageFlighDelayMapper extends
			Mapper<Object, Text, Text, Text> {

		// CSVParser uses ',' as separator and '"' as char quote
		private CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {

			String[] record = this.csvParser.parseLine(value.toString());
			Text key = new Text();
			Text item = new Text();

			// Select only valid records
			if (record.length > 0 && isValidRecord(record)) {

				// Set item value as (Flag, DepTime, ArrTime, ArrDelayMinutes)
				String itemValue = setItemValue(record);
				item.set(itemValue);

				// Set key value as (FlightDate,IntermediateAirPort)
				if (itemValue.contains(ORIGIN)) {
					key.set((record[FLIGHTDATE_COLUMNINDEX] + "," + record[DEST_COLUMNINDEX])
							.toUpperCase());
				} else {
					key.set((record[FLIGHTDATE_COLUMNINDEX] + "," + record[ORIGIN_COLUMNINDEX])
							.toUpperCase());
				}
				context.write(key, item);
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
			if (record[FLIGHTDATE_COLUMNINDEX].isEmpty()
					|| record[ORIGIN_COLUMNINDEX].isEmpty()
					|| record[DEST_COLUMNINDEX].isEmpty()
					|| record[DEPTIME_COLUMNINDEX].isEmpty()
					|| record[ARRTIME_COLUMNINDEX].isEmpty()
					|| record[ARRDELAYMINUTES_COLUMNINDEX].isEmpty()
					|| record[CANCELLED_COLUMNINDEX].isEmpty()
					|| record[DIVERTED_COLUMNINDEX].isEmpty()) {
				return false;
			}

			// If the flight date does not fall into the time period, return false
			SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd");
			try {
				Date flightDate = dateParser
						.parse(record[FLIGHTDATE_COLUMNINDEX]);

				if (flightDate.before(dateParser.parse(PERIOD_STARTDATE))
						|| flightDate.after(dateParser.parse(PERIOD_ENDDATE))) {
					return false;
				}
			} catch (ParseException e) {
				return false;
			}

			// If neither the origin nor the destination matches, return false
			if (!record[ORIGIN_COLUMNINDEX].toUpperCase().equals(ORIGIN)
					&& !record[DEST_COLUMNINDEX].toUpperCase().equals(
							DESTINATION)) {
				return false;
			}

			// If the origin and the destination match at the same time, return false
			// Because in this case, the flight is considered as one-leg flight
			if (record[ORIGIN_COLUMNINDEX].toUpperCase().equals(ORIGIN)
					&& record[DEST_COLUMNINDEX].toUpperCase().equals(
							DESTINATION)) {
				return false;
			}

			// If the flight was cancelled or diverted, return false
			if (record[CANCELLED_COLUMNINDEX].equals("1")
					|| record[DIVERTED_COLUMNINDEX].equals("1")) {
				return false;
			}
			return true;
		}

		/**
		 * The function generates the output item value
		 * 
		 * @return item string separating attributes by comma
		 */
		private String setItemValue(String[] record) {
			StringBuilder item = new StringBuilder();

			if (record[ORIGIN_COLUMNINDEX].toUpperCase().equals(ORIGIN)) {
				// Set Flag attribute of the item which has the matching origin
				item.append(ORIGIN).append(",");
			} else {
				// Set Flag attribute of the item which has the matching destination
				item.append(DESTINATION).append(",");
			}
			item.append(record[DEPTIME_COLUMNINDEX]).append(","); // Set DepTime
			item.append(record[ARRTIME_COLUMNINDEX]).append(","); // Set ArrTime
			item.append(record[ARRDELAYMINUTES_COLUMNINDEX]); // Set ArrDelayMinutes

			return item.toString();
		}
	}

	/**
	 * The Reducer applies equi-join on the Mapper outputs
	 */
	public static class AverageFlightDelayReducer extends
			Reducer<Text, Text, Text, Text> {

		private CSVParser csvParser;
		private int sumFlights;
		private float sumDelay;

		/**
		 * Initialization
		 */
		protected void setup(Context context) {

			// CSVParser uses ',' as separator and '"' as char quote
			this.csvParser = new CSVParser(',', '"');
			this.sumDelay = 0;
			this.sumFlights = 0;
		}

		/**
		 * The key value pair is (FlightDate,IntermediateAirPort) So the values
		 * are of the same date and the same intermediate airport
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<String> originFlights = new ArrayList<String>();
			List<String> destinFlights = new ArrayList<String>();

			// Separate originFlights and destinFlights
			for (Text value : values) {
				String item = value.toString();
				if (item.contains(ORIGIN)) {
					originFlights.add(item);
				} else {
					destinFlights.add(item);
				}
			}

			for (String originFlight : originFlights) {
				for (String destinFlight : destinFlights) {
					String[] originInfo = this.csvParser
							.parseLine(originFlight);
					String[] destinInfo = this.csvParser
							.parseLine(destinFlight);

					// Calculate all verified two-leg flights' delay
					if (isTwoLegFlight(originInfo, destinInfo)) {
						float delay = Float.parseFloat(originInfo[3])
								+ Float.parseFloat(destinInfo[3]);
						this.sumDelay += delay;
						this.sumFlights++;
					}
				}
			}
		}

		/**
		 * Validate whether origin and destination information is valid item for
		 * a two-leg flight
		 * 
		 * @param originInfo
		 *            : Information of a flight whose flag is ORIGIN
		 * @param destinInfo
		 *            : Information of a flight whose flag is DESTINATION
		 */
		private boolean isTwoLegFlight(String[] originInfo, String[] destinInfo) {

			// The DepTime of destinFlight must be later than the ArrTime of originFlight
			String arrTime = originInfo[2];
			String depTime = destinInfo[1];
			if (Integer.parseInt(arrTime) < Integer.parseInt(depTime)) {
				return true;
			}
			return false;
		}

		/**
		 * Write the final sumDelay and sumFlights for each reduce task
		 */
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(new Text("" + sumDelay), new Text("" + sumFlights));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AverageFlighDelay <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf,
				"Compute the average flight delay for 2-legged flights");
		/*
		 * The line below can set the amount of reduce tasks to a specific
		 * number, which equals to the amount of output 'part-r-...' files
		 */
		job.setNumReduceTasks(10);
		job.setJarByClass(AverageFlightDelay.class);
		job.setMapperClass(AverageFlighDelayMapper.class);
		job.setReducerClass(AverageFlightDelayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
