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

public class PageRank {
    public static class PageRankMapper
            extends Mapper<LongWritable, Text, LongWritable,PRNodeWritable> {

        public void map(LongWritable key, Text t, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long count = Long.valueOf(conf.get("reachCount"));
            PRNodeWritable pr = new PRNodeWritable();
            long nid = pr.getByText(t);
            LongWritable nidWritable = new LongWritable(nid);

            Text adjList = pr.getAdjList();
            long[] adjs = PRNodeWritable.stringToArray(adjList);

            DoubleWritable pageRankWritable = pr.getDistance();
            double pageRank = pageRankWritable.get();
            if(pageRank < 0){
                pageRank = 1/count;
            }
            double p = pageRank/adjs.length;
            DoubleWritable pWritable = new DoubleWritable(p);
            pr.setDistance(pWritable);
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
//                if (node.getFlag().get())
//                {
//                    infoNode.copy(node, key);
//                }
//                else
//                {
//                    res += node.getDistance().get();
//                }
                context.write(key, node);
            }
//            infoNode.setDistance(new DoubleWritable(res));
//            context.write(key, infoNode);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "PRPreProcess");
        job1.setJarByClass(PRPreProcess.class);
        job1.setMapperClass(PRPreProcess.PRPreProMapper.class);
//        job.setCombinerClass(PDPreProcess.PDPreProReducer.class);
        job1.setReducerClass(PRPreProcess.PRPreProReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(PRNodeWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path("/user/hadoop/pr/tmp/Output0/"));
        ControlledJob cjob1 = new ControlledJob(conf1);

        cjob1.setJob(job1);
        JobControl jc = new JobControl("PRPreProcess");
        jc.addJob(cjob1);

        Thread jcThread = new Thread(jc);
        jcThread.start();
        while(true){
            if(jc.allFinished()){
                System.out.println(jc.getSuccessfulJobList());
                System.out.println(jc.getFailedJobList());
                jc.stop();
                break;
            }
        }
        long reachCount= job1.getCounters().findCounter(PRPreProcess.PRPreProReducer.ReachCounter.COUNT).getValue();

        String itr = args[0];
        String threshold = args[1];
        int i = 0;
        int iteration = Integer.parseInt(itr);
        int iterNum = 0;
        Configuration conf2 = new Configuration();
        conf2.set("reachCount", String.valueOf(reachCount));
        conf2.set("threshold", threshold);

        Job job2 = Job.getInstance(conf2, "PageRank");
        job2.setJarByClass(PageRank.class);
        job2.setMapperClass(PageRankMapper.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(PRNodeWritable.class);
//        job2.setCombinerClass(PageRankReducer.class);
        job2.setReducerClass(PageRankReducer.class);
        //设置reduce输出的key和value类型
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(PRNodeWritable.class);

        while(i < iteration) {
            FileInputFormat.setInputPaths(job2, new Path("/user/hadoop/pr/tmp/Output" + i));
            i++;
            if (i < iteration - 1)
                FileOutputFormat.setOutputPath(job2, new Path("/user/hadoop/pr/tmp/Output" + i));
            else
                FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            ControlledJob cjob2 = new ControlledJob(conf2);

            cjob2.setJob(job2);
            jc = new JobControl("PageRank");
            jc.addJob(cjob2);

            jcThread = new Thread(jc);
            jcThread.start();
            while (true) {
                if (jc.allFinished()) {
                    System.out.println(jc.getSuccessfulJobList());
                    System.out.println(jc.getFailedJobList());
                    jc.stop();
                    break;
                }
            }
        }

    }
}
