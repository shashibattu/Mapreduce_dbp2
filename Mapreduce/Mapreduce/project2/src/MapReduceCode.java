import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MapReduceCode {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text bookId = new Text();
        private Text bookName = new Text();
        private final static IntWritable score = new IntWritable(1); // Fixed score of 1 for counting

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length >= 2) {
                bookId.set(parts[0].trim()); // Assuming the first column is Book ID
                bookName.set(parts[1].trim()); // Assuming the second column is Book Name
                context.write(new Text(bookId.toString() + " - " + bookName.toString()), score); // Emit bookId-bookName pair with a score of 1 for counting
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get(); // Summing up the scores which are all 1
            }
            context.write(key, new IntWritable(count)); // Emit the bookId-bookName pair with the total count
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "book review analysis");
        job.setJarByClass(MapReduceCode.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); // Output value is IntWritable for count
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
