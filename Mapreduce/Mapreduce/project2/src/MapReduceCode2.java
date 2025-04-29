import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.HashSet;

public class MapReduceCode2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text bookName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length >= 2) {
                String bookNameValue = parts[1].trim(); // Assuming the second column is Book Name
                bookName.set(bookNameValue);
                context.write(bookName, NullWritable.get()); // Emit book name with a NullWritable value
            }
        }
    }

    public static class DistinctBookNamesReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            // Emit each distinct book name
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distinct book names");
        job.setJarByClass(MapReduceCode2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DistinctBookNamesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
