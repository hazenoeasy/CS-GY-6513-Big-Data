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
 * @author hazenoeasy
 */
public class WordFind {
    private static Path outputPath;

    // this is the driver
    public static void main(String[] args) throws Exception {

        // get a reference to a jobCount runtime configuration for this program
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: <in_dir> <count_dir> <prob_dir> <final_dir> ");
            System.exit(2);
        }
        // if output fold exists, delete it.
        FileSystem fileSystem = FileSystem.get(conf);
        for (int i = 1; i <= 3; i++) {
            if (fileSystem.exists(new Path(otherArgs[i]))) {
                // 第二个参数代表循环删除
                fileSystem.delete(new Path(otherArgs[i]), true);
            }
        }

        // job:  words count
        @SuppressWarnings("deprecation") Job jobCount = new Job(conf, "word count");
        jobCount.setJarByClass(WordFind.class);
        jobCount.setMapperClass(CountMapper.class);
        jobCount.setReducerClass(CountReducer.class);
        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(jobCount, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(jobCount, new Path(otherArgs[1]));
        jobCount.waitForCompletion(true);

        // job: Count the number of 3 types gram
        Configuration conf2 = new Configuration();
        // read by line
        conf2.set("mapreduce.job.inputformat.class", "org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat");
        @SuppressWarnings("deprecation") Job jobProb = new Job(conf2, "word prob");
        jobProb.setJarByClass(WordFind.class);
        jobProb.setMapperClass(ProbMapper.class);
        jobProb.setReducerClass(ProbReducer.class);
        jobProb.setOutputKeyClass(Text.class);
        jobProb.setOutputValueClass(IntWritable.class);
        // set 3 reducers
        jobProb.setNumReduceTasks(3);
        FileInputFormat.addInputPath(jobProb, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(jobProb, new Path(otherArgs[2]));
        jobProb.waitForCompletion(true);

        // read 3 counter from the hdfs file
        outputPath = FileOutputFormat.getOutputPath(jobProb);
        Text[] amount = getCount(conf, outputPath.toString());
        // save counters int global configuration
        conf2.set("count1", amount[0].toString());
        conf2.set("count2", amount[1].toString());
        conf2.set("count3", amount[2].toString());
        //job: merge counters into word count file
        @SuppressWarnings("deprecation") Job jobFormat = new Job(conf2, "word format");
        FileInputFormat.addInputPath(jobFormat, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(jobFormat, new Path(otherArgs[3]));
        jobFormat.setJarByClass(WordFind.class);
        // all can be done in mapper, so we don't need reducer
        jobFormat.setNumReduceTasks(0);
        jobFormat.setMapperClass(FormatMapper.class);
        System.exit(jobFormat.waitForCompletion(true) ? 0 : 1);
    }

    public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        // save 2-gram word
        private Deque<String> deque1 = new ArrayDeque<>();
        // save 3-gram word
        private Deque<String> deque2 = new ArrayDeque<>();
        // regex filter
        public static String regex = "[^a-zA-Z0-9]";

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
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                deque1.addLast(nextToken);
                deque2.addLast(nextToken);

                // write 1-gram word
                word.set(nextToken);
                context.write(word, one);

                // write 2-gram word
                if (deque1.size() == 2) {
                    word.set(String.join(" ", deque1));
                    deque1.removeFirst();
                    context.write(word, one);
                }

                //write 3-gram word
                if (deque2.size() == 3) {
                    word.set(String.join(" ", deque2));
                    deque2.removeFirst();
                    context.write(word, one);
                }
            }
            // clean buffer
            deque1.clear();
            deque2.clear();
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
        private Text word = new Text();
        private IntWritable result = new IntWritable();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            result.set(Integer.valueOf(value.toString()));
            // length is the gram type
            int length = key.toString().trim().split(" ").length;
            word.set(length + "");
            // write gram type and amount
            context.write(word, result);
        }
    }

    public static class ProbReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static class FormatMapper extends Mapper<Text, Text, Text, Text> {
        Text word = new Text();
        private static String[] mount = new String[3];

        // read from configuration
        @Override
        protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            mount[0] = configuration.get("count1");
            mount[1] = configuration.get("count2");
            mount[2] = configuration.get("count3");
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // length is the gram type
            int length = key.toString().split(" ").length;
            // concat value with "/" and gram amount
            word.set(value.toString() + "/" + mount[length - 1].toString());
            context.write(key, word);
        }
    }

    // read counters from hdfs
    private static Text[] getCount(Configuration conf, String path) throws IOException {
        Text[] result = {new Text(), new Text(), new Text()};
        FileSystem fs = FileSystem.get(conf);
        // counter store in 3 files : part-r-00000 part-r-00001 part-r-00002
        for (int i = 0; i < 3; i++) {
            Path file = new Path(path + "/part-r-0000" + i);
            FSDataInputStream fsDataInputStream = null;
            try {
                // open hdfs stream
                fsDataInputStream = fs.open(file);
                // read buffer
                byte[] buf = new byte[1024];
                // load data into buffer
                int readLen = fsDataInputStream.read(buf);
                String s = new String(buf, 0, readLen);
                // split data into gram type and gram amount
                String[] split = s.trim().split("\t");
                Integer index = Integer.valueOf(split[0].trim());
                String count = split[1].trim();
                System.out.println("xxxx: " + count);
                // save in result[]
                result[index - 1].set(count);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                // close
                if (fsDataInputStream != null) {
                    fsDataInputStream.close();
                }
            }
        }
        return result;
    }
}
