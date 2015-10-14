package hw1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapOnlyJoin {
    public static class ReplicatedJoinMapper extends
            Mapper<Object, Text, Text, Text> {

        private HashSet<String> set = new HashSet<String>();

        private Text outputKey = new Text();
        private Text outvalue = new Text();

		private BufferedReader rdr;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            try {
                Path[] files = {new Path("C:/My Applications/workspace/input/aa.txt")};
                if (files == null || files.length == 0) {
                    throw new RuntimeException(
                            "User information is not set in DistributedCache");
                }

                // Read all files in the DistributedCache
                for (Path p : files) {
                    rdr = new BufferedReader(
                            new InputStreamReader(
                                    new FileInputStream(
                                            new File(p.toString()))));

                    String line;
                    // For each record in the user file
                    while ((line = rdr.readLine()) != null) {
                        set.add(line);
                    }
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Get the join type
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String str = value.toString();
            // If the user information is not null, then output
            if (set.contains(str)) {
                outputKey.set(str);
                context.write(outputKey, outvalue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err
                    .println("Usage: ReplicatedJoin <in> <out>");
            System.exit(1);
        }


        // Configure the join type
        Job job = new Job(conf, "Replicated Join");
        job.setJarByClass(MapOnlyJoin.class);

        job.setMapperClass(ReplicatedJoinMapper.class);
        job.setNumReduceTasks(0);

        TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure the DistributedCache
        System.exit(job.waitForCompletion(true) ? 0 : 3);
    }

}
