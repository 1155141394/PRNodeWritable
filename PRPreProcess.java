import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.ArrayWritable;

public class PRPreProcess {
    public static class PRPreProMapper
            extends Mapper<Object, Text, IntWritable, MapWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

        }
    }

    public static class PRPreProReducer
            extends Reducer<IntWritable,MapWritable,IntWritable,PDNodeWritable> {
        private IntWritable point = new IntWritable();

        public void reduce(IntWritable key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

        }
    }

}
