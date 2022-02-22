import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.math.BigInteger;

/**
 * @author Yuh Z
 * @date 2/21/22
 */
public class testFileReading {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: gram <in_dir> <count_dir> <prob_dir> ");
            System.exit(2);
        }
        String[] split = getCount(conf, otherArgs[2]);
        System.out.println(split);
    }

    private static String[] getCount(Configuration conf, String path) throws IOException {
        String[] result = new String[3];
        FileSystem fs = FileSystem.get(conf);
        for (int i = 0; i < 3; i++) {
            Path file = new Path(path + "/part-r-0000" + i);
            FSDataInputStream fsDataInputStream = fs.open(file);
            byte[] buf = new byte[1024];
            int readLen = fsDataInputStream.read(buf);
            fsDataInputStream.close();
            String s = new String(buf, 0, readLen);
            String[] split = s.trim().split("\t");
            Integer index = Integer.valueOf(split[0].trim());
            String count = split[1].trim();
            result[index - 1] = count;
        }
        return result;
    }
}
