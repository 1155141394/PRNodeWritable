import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
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
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            IntWritable startPoint = new IntWritable();
            IntWritable endPoint = new IntWritable();
            StringTokenizer itr = new StringTokenizer(value.toString());
            int u = Integer.valueOf(itr.nextToken());
            int v = Integer.valueOf(itr.nextToken());
            startPoint.set(u);
            endPoint.set(v);
            context.write(startPoint, endPoint);
        }
    }

    public static class PRPreProReducer
            extends Reducer<IntWritable,IntWritable,IntWritable, PRNodeWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            PRNodeWritable resNode = new PRNodeWritable();
            Text resText = new Text();
            String resStr = "";
            for (IntWritable endPoint : values){
                resStr = resStr + String.valueOf(endPoint);
                resStr = resStr + ",";
            }
            resStr = resStr.substring(0,resStr.length()-1);
            resText.set(resStr);
            resNode.set(new DoubleWritable(1.0),resText,new BooleanWritable(true));
            context.write(key, resNode);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("src", args[2]);
        Job job = Job.getInstance(conf, "PRPreProcess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreProcess.PRPreProMapper.class);
//        job.setCombinerClass(PDPreProcess.PDPreProReducer.class);
        job.setReducerClass(PRPreProcess.PRPreProReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
