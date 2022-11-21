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
            extends Mapper<LongWritable, Text, LongWritable,PRNodeWritable> {

        public void map(LongWritable key, Text t, Context context
        ) throws IOException, InterruptedException {
            PRNodeWritable pr = new PRNodeWritable();
            long nid = (long)pr.getByText(t);
            LongWritable nidWritable = new LongWritable(nid);

            Text adjList = pr.getAdjList();
            long[] adjs = PRNodeWritable.stringToArray(adjList);

            DoubleWritable pageRankWritable = pr.getDistance();
            double pageRank = pageRankWritable.get();
            double p = pageRank/adjs.length;

            context.write(nidWritable,pr);
            for(long adj : adjs){
                LongWritable adjWritable = new LongWritable(adj);
                Text tmp = new Text();
                BooleanWritable flag = new BooleanWritable(false);
                PRNodeWritable N = new PRNodeWritable();
                N.set(new DoubleWritable(p),tmp,flag);
                context.write(adjWritable, N);
            }
        }
    }

    public static class PageRankReducer
            extends Reducer<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {

        public void reduce(LongWritable key, Iterable<PRNodeWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double res = 0.0;
            PRNodeWritable infoNode = new PRNodeWritable();
            for (PRNodeWritable node: values)
            {
                if (node.getFlag().get())
                {
                    infoNode.copy(node, key);
                }
                else
                {
                    res += node.getDistance().get();
                }
            }
            infoNode.setDistance(new DoubleWritable(res));
            context.write(key, infoNode);
        }
    }

    public static void main(String[] args) throws Exception {
        
    }
}
