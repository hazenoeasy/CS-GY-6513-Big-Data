package yuhao.www;
// namespace
//java dependencioes

import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;

// hadoop dependencies
//  maven: org.apache.hadoop:hgadoop-client:x.y.z  (x..z = hadoop version, 3.3.0 here
import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//our program. This program will be 'started' by the Hadoop runtime
public class WordCount {
    public static String regex = "[^a-zA-Z0-9]";

    // this is the driver
    public static void main(String[] args) throws Exception {

        // get a reference to a job runtime configuration for this program
        Configuration conf = new Configuration();
        //
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        //        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // read by line
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // this class implements the mapper
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // regex   delete all illegal character
            String string = value.toString();
            Pattern compile = Pattern.compile(regex);
            String trim = compile.matcher(string).replaceAll(" ").trim();
            trim = trim.toLowerCase(Locale.ROOT);
            StringTokenizer itr = new StringTokenizer(trim);
            //ignore Ignore lines with less than 3 words.
            if (itr.countTokens() < 3) {
                return;
            }
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                word.set(nextToken);

                context.write(word, one);
            }
        }
    }

    // this class implements the reducer
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
