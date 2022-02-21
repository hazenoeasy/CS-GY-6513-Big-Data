import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author Yuh Z
 * @date 2/21/22
 */
public class testFileReading {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 4) {
            System.err.println("Usage: gram <n> <in_dir> <count_dir> <prob_dir> ");
            System.exit(2);
        }
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(otherArgs[3] + "/part-r-00000");
        FSDataInputStream fsDataInputStream = fs.open(file);
        byte[] buf = new byte[1024];
        int readLen = fsDataInputStream.read(buf);
         fsDataInputStream.close();
        String s = new String(buf, 0, readLen);
        System.out.println(Integer.valueOf(s.trim()));
        System.out.println(s);
    }
}
