package yuhao.www;

import java.awt.event.KeyAdapter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.Pattern;

/**
 * @author Yuh Z
 * @date 2/22/22
 */

public class UnitedState {
    private static Path outputPath;

    // this is the driver
    public static void main(String[] args) throws Exception {

        // get a reference to a jobFind runtime configuration for this program
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in_dir> <intermid_dir> <result_dir>");
            System.exit(2);
        }
        FileSystem fileSystem = FileSystem.get(conf);

        // if output fold exists, delete it.
        if (fileSystem.exists(new Path(otherArgs[1]))) {
            // 第二个参数代表循环删除
            fileSystem.delete(new Path(otherArgs[1]), true);
        }
        conf.set("mapreduce.job.inputformat.class", "org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat");
        @SuppressWarnings("deprecation") Job jobFind = new Job(conf, "word find");
        jobFind.setJarByClass(UnitedState.class);
        jobFind.setMapperClass(FindMapper.class);
        jobFind.setReducerClass(FindReducer.class);
        jobFind.setOutputKeyClass(Text.class);
        jobFind.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(jobFind, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(jobFind, new Path(otherArgs[1]));
        jobFind.waitForCompletion(true);
        System.exit(0);

    }

    public static class FindMapper extends Mapper<Text, Text, Text, Text> {
        public String key1 = "united"; // encyclopaedia britannica
        public String key2 = "states";
        private Text word = new Text();
        // all pairs should have same key.
        private Text keyReturn = new Text(key1 + " " + key2);

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // split the key into words
            String[] keyArray = key.toString().trim().split(" ");
            // if the key is 3-gram and match key1 and key2
            if (keyArray.length == 3 && keyArray[0].equals(key1) && keyArray[1].equals(key2)) {
                // write value with X word and  how many times it appears.
                word.set(keyArray[2] + " " + value.toString().trim());
                context.write(keyReturn, word);
            }
        }
    }

    public static class FindReducer extends Reducer<Text, Text, Text, IntWritable> {

        private IntWritable max = new IntWritable(0);
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (!key.toString().trim().equals("united states")) {
                return;
            }
            // find the highest probability word
            for (Text val : values) {
                String[] strings = val.toString().trim().split(" ");
                IntWritable count = new IntWritable(Integer.valueOf(strings[1].trim()));
                if (max.compareTo(count) < 0) {
                    max = count;
                    result.set(strings[0]);
                }
            }
            context.write(result, max);
        }
    }
}

