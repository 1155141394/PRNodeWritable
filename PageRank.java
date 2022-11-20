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

public class PageRank {
    public static class PageRankMapper
            extends Mapper<LongWritable, Text, LongWritable,PDNodeWritable> {

        public void map(LongWritable key, Text t, Context context
        ) throws IOException, InterruptedException {

        }
    }

    public static class PageRankReducer
            extends Reducer<LongWritable,PDNodeWritable,LongWritable,PDNodeWritable> {

        public void reduce(LongWritable key, Iterable<PDNodeWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        
    }
}