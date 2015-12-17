package hw4;

import java.io.IOException;
import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This class reads records from the input file and writes each record 1-to-1 to
 * an HBase table.
 */
public class HPopulate {

	private static final String TABLE_NAME = "FlightData";
	private static final String COLUMN_FAMILY = "FlightInfo";
	private static final String COLUMN_ARRDELAYMINUTES = "ArrDelayMinutes";
	private static final int YEAR_COLUMNINDEX = 0;
	private static final int MONTH_COLUMNINDEX = 2;
	private static final int DAYOFMONTH_COLUMNINDEX = 3;
	private static final int UNIQUECARRIER_COLUMNINDEX = 6;
	private static final int FLIGHTNUM_COLUMNINDEX = 10;
	private static final int ORIGIN_COLUMNINDEX = 11;
	private static final int ARRDELAYMINUTES_COLUMNINDEX = 37;

	public static class HPopulateMapper extends
			Mapper<Object, Text, ImmutableBytesWritable, Writable> {

		private CSVParser csvParser = null;
		private HTable table = null;

		/**
		 * Initialize csvParser and HBase table connection
		 */
		protected void setup(Context context) throws IOException {

			// CSVParser uses ',' as separator and '"' as char quote
			this.csvParser = new CSVParser(',', '"');

			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, TABLE_NAME);
			table.setAutoFlush(false);
			// Set 10 MB write buffer size
			table.setWriteBufferSize(10 * 1024 * 1024); 
		}

		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = this.csvParser.parseLine(value.toString());
			StringBuilder rowKey = new StringBuilder();

			if (line.length > 0 && !line[ARRDELAYMINUTES_COLUMNINDEX].isEmpty()) {

				/**
				 * Set rowKey as
				 * (Year,Month,DayofMonth,UniqueCarrier,FlightNum,Origin).
				 * Select Year as prefix to take advantage of HBase built-in
				 * sorting functionality. Month and UniqueCarrier are required
				 * attributes for HCompute. Others are used to generate unique
				 * rowKey
				 */
				rowKey.append(line[YEAR_COLUMNINDEX] + ",")
						.append(line[MONTH_COLUMNINDEX] + ",")
						.append(line[DAYOFMONTH_COLUMNINDEX] + ",")
						.append(line[UNIQUECARRIER_COLUMNINDEX] + ",")
						.append(line[FLIGHTNUM_COLUMNINDEX] + ",")
						.append(line[ORIGIN_COLUMNINDEX]);

				String delay = line[ARRDELAYMINUTES_COLUMNINDEX];

				Put record = new Put(rowKey.toString().getBytes());
				record.add(COLUMN_FAMILY.getBytes(),
						COLUMN_ARRDELAYMINUTES.getBytes(), delay.getBytes());

				table.put(record);
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			table.close();
		}
	}

	/**
	 * Connect to HBase instance and create a new table. If the table already
	 * exists, then delete the existing table and create a new one
	 */
	public static void createHTable() throws IOException,
			ZooKeeperConnectionException {
		Configuration conf = HBaseConfiguration.create();
		HTableDescriptor htable = new HTableDescriptor(TABLE_NAME);
		htable.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(TABLE_NAME)) {
			admin.disableTable(TABLE_NAME);
			admin.deleteTable(TABLE_NAME);
		}
		admin.createTable(htable);
		admin.close();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: H-POPULATE <in>");
			System.exit(2);
		}

		// Create HTable
		createHTable();

		Job job = new Job(conf, "H-POPULATE");
		job.setJarByClass(HPopulate.class);
		job.setMapperClass(HPopulateMapper.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

		// Set the number of reduce task to 0 explicitly
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
