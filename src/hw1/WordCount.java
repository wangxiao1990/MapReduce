/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private HashMap<Text, IntWritable> h;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			h = new HashMap<Text, IntWritable>();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				// Get the first letter of the word
				int c = word.charAt(0);
				/*
				 * Make sure the real words starts with letters 'm','n','o','p'
				 * and 'q' (no matter if they are capitalized or not)
				 */
				if ((c >= 'M' && c <= 'Q') || (c >= 'm' && c <= 'q')) {
					Text w = new Text(word);
					if (h.containsKey(w)) {
						IntWritable count = h.get(w);
						count.set(count.get() + 1);
						h.put(w, count);
					} else {
						h.put(w, new IntWritable(1));
					}
				}
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Map.Entry<Text, IntWritable> entry : h.entrySet()) {
				context.write(entry.getKey(), entry.getValue());
			}
		}
	}

	public static class CustomPartitioner extends
			Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			// Get the first letter of the key
			int c = key.charAt(0);
			// The index of the task it should assign to
			int index = 0;
			if (c == 'm' || c == 'M') {
				index = 0 % numPartitions;
			}
			if (c == 'n' || c == 'N') {
				index = 1 % numPartitions;
			}
			if (c == 'o' || c == 'O') {
				index = 2 % numPartitions;
			}
			if (c == 'p' || c == 'P') {
				index = 3 % numPartitions;
			}
			if (c == 'q' || c == 'Q') {
				index = 4 % numPartitions;
			}
			return index;
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		/*
		 * The line below can set the amount of reduce tasks to a specific
		 * number, which equals to the amount of output 'part-r-...' files
		 */
		// job.setNumReduceTasks(5);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}