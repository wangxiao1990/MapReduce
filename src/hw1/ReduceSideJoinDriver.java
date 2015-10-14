package hw1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoinDriver {

	public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			StringTokenizer itr = new StringTokenizer(value.toString());
			outkey.set(itr.nextToken());
			outvalue.set("A");
			context.write(outkey, outvalue);

		}
	}

	public static class CommentJoinMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			StringTokenizer itr = new StringTokenizer(value.toString());
			outkey.set(itr.nextToken());
			outvalue.set("B");
			context.write(outkey, outvalue);

		}
	}

	public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		private String joinType = null;

		@Override
		public void setup(Context context) {
			// Get the type of join from our configuration
			joinType = context.getConfiguration().get("join.type");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Clear our lists
			// listA.clear();
			// listB.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			for (Text t : values) {
				if (t.toString().charAt(0) == 'A') {
					listA.add(new Text(key));
				} else if (t.toString().charAt(0) == 'B') {
					listB.add(new Text(key));
				}
			}

			// Execute our join logic now that the lists are filled
			// executeJoinLogic(context);
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (Text A : listA) {
					if (listB.contains(A)) {
						context.write(A, null);
					}
				}
			}
		}

		private void executeJoinLogic(Context context) throws IOException,
				InterruptedException {
			if (joinType.equalsIgnoreCase("inner")) {
				// If both lists are not empty, join A with B
				if (!listA.isEmpty() && !listB.isEmpty()) {
					for (Text A : listA) {
						for (Text B : listB) {
							context.write(A, B);
						}
					}
				}
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				// For each entry in A,
				for (Text A : listA) {
					// If list B is not empty, join A and B
					if (!listB.isEmpty()) {
						for (Text B : listB) {
							context.write(A, B);
						}
					} else {
						// Else, output A by itself
						context.write(A, new Text(""));
					}
				}
			} else if (joinType.equalsIgnoreCase("rightouter")) {
				// FOr each entry in B,
				for (Text B : listB) {
					// If list A is not empty, join A and B
					if (!listA.isEmpty()) {
						for (Text A : listA) {
							context.write(A, B);
						}
					} else {
						// Else, output B by itself
						context.write(new Text(""), B);
					}
				}
			} else if (joinType.equalsIgnoreCase("fullouter")) {
				// If list A is not empty
				if (!listA.isEmpty()) {
					// For each entry in A
					for (Text A : listA) {
						// If list B is not empty, join A with B
						if (!listB.isEmpty()) {
							for (Text B : listB) {
								context.write(A, B);
							}
						} else {
							// Else, output A by itself
							context.write(A, new Text(""));
						}
					}
				} else {
					// If list A is empty, just output B
					for (Text B : listB) {
						context.write(new Text(""), B);
					}
				}
			} else if (joinType.equalsIgnoreCase("anti")) {
				// If list A is empty and B is empty or vice versa
				if (listA.isEmpty() ^ listB.isEmpty()) {

					// Iterate both A and B with null values
					// The previous XOR check will make sure exactly one of
					// these lists is empty and therefore won't have output
					for (Text A : listA) {
						context.write(A, new Text(""));
					}

					for (Text B : listB) {
						context.write(new Text(""), B);
					}
				}
			} else {
				throw new RuntimeException(
						"Join type not set to inner, leftouter, rightouter, fullouter, or anti");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: ReduceSideJoin <user data> <comment data> <out> [inner|leftouter|rightouter|fullouter|anti]");
			System.exit(1);
		}

		String joinType = otherArgs[3];
		if (!(joinType.equalsIgnoreCase("inner")
				|| joinType.equalsIgnoreCase("leftouter")
				|| joinType.equalsIgnoreCase("rightouter")
				|| joinType.equalsIgnoreCase("fullouter") || joinType
					.equalsIgnoreCase("anti"))) {
			System.err
					.println("Join type not set to inner, leftouter, rightouter, fullouter, or anti");
			System.exit(2);
		}

		Job job = new Job(conf, "Reduce Side Join");

		// Configure the join type
		job.getConfiguration().set("join.type", joinType);
		job.setJarByClass(ReduceSideJoinDriver.class);

		// Use multiple inputs to set which input uses what mapper
		// This will keep parsing of each data set separate from a logical
		// standpoint
		// However, this version of Hadoop has not upgraded MultipleInputs
		// to the mapreduce package, so we have to use the deprecated API.
		// Future releases have this in the "mapreduce" package.
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, UserJoinMapper.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, CommentJoinMapper.class);

		job.setReducerClass(UserJoinReducer.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
