package yuhao.www;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author hazenoeasy
 */
public class WordCount {
    public static String regex = "[^a-zA-Z0-9]";
    private static int N;
    private static int amount;

    // this is the driver
    public static void main(String[] args) throws Exception {

        // get a reference to a jobCount runtime configuration for this program
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5) {
            System.err.println("Usage: gram <n> <in_dir> <count_dir> <prob_dir> <final_dir> ");
            System.exit(2);
        }
        N = Integer.valueOf(otherArgs[0]);

        @SuppressWarnings("deprecation") Job jobCount = new Job(conf, "word count");
        jobCount.setJarByClass(WordCount.class);
        jobCount.setMapperClass(CountMapper.class);
        jobCount.setReducerClass(CountReducer.class);
        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(jobCount, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(jobCount, new Path(otherArgs[2]));
        jobCount.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.job.inputformat.class", "org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat");
        @SuppressWarnings("deprecation") Job jobProb = new Job(conf2, "word prob");
        jobProb.setJarByClass(WordCount.class);
        jobProb.setMapperClass(ProbMapper.class);
        jobProb.setReducerClass(ProbReducer.class);
        jobProb.setOutputKeyClass(Text.class);
        jobProb.setOutputValueClass(IntWritable.class);
        // read by line
        FileInputFormat.addInputPath(jobProb, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(jobProb, new Path(otherArgs[3]));
        jobProb.waitForCompletion(true);

        @SuppressWarnings("deprecation") Job jobFormat = new Job(conf2, "word format");
        FileInputFormat.addInputPath(jobFormat, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(jobFormat, new Path(otherArgs[4]));
        FileSystem fs = FileSystem.get(conf2);
        Path file = new Path(otherArgs[3] + "/part-r-00000");
        FSDataInputStream fsDataInputStream = fs.open(file);
        byte[] buf = new byte[1024];
        int readLen = fsDataInputStream.read(buf);
        fsDataInputStream.close();
        amount = Integer.valueOf(new String(buf, 0, readLen).trim());
        jobFormat.setJarByClass(WordCount.class);
        jobFormat.setNumReduceTasks(0);
        jobFormat.setMapperClass(FormatMapper.class);
        System.exit(jobFormat.waitForCompletion(true) ? 0 : 1);
    }

    public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Deque<String> deque = new ArrayDeque<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // regex   delete all illegal character
            String string = value.toString();
            Pattern compile = Pattern.compile(regex);
            String trim = compile.matcher(string).replaceAll(" ").trim();
            trim = trim.toLowerCase(Locale.ROOT);
            StringTokenizer itr = new StringTokenizer(trim);
            //ignore lines with less than 3 words.
            if (itr.countTokens() < 3) {
                return;
            }
            for (int i = 1; i < N; i++) {
                deque.addLast(itr.nextToken());
            }
            while (itr.hasMoreTokens()) {
                deque.addLast(itr.nextToken());
                word.set(String.join(" ", deque));
                context.write(word, one);
                deque.removeFirst();
            }
            deque.clear();
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int localSum = 0;
            for (IntWritable val : values) {
                localSum += val.get();
            }
            result.set(localSum);
            context.write(key, result);
        }
    }

    public static class ProbMapper extends Mapper<Text, Text, Text, IntWritable> {
        private Text word = new Text("*");
        private IntWritable result = new IntWritable();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            result.set(Integer.valueOf(value.toString()));
            context.write(word, result);
        }
    }

    public static class ProbReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int localSum = 0;
            for (IntWritable val : values) {
                localSum += val.get();
            }
            result.set(localSum);
            context.write(word, result);
        }
    }

    public static class FormatMapper extends Mapper<Text, Text, Text, Text> {
        Text word = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            word.set(value.toString() + "/" + amount);
            context.write(key, word);
        }
    }
}
