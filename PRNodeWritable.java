import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*; // 引入 ArrayList 类
import java.util.HashMap;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Public
@InterfaceStability.Stable

public class PRNodeWritable implements Writable {
    // Some data
    private FloatWritable distance = new FloatWritable(1);
    private ArrayWritable adjList = new ArrayWritable();

    public BooleanWritable flag = new BooleanWritable(true);

    public void PRNodeWritable() throws IOException {
        this.distance = new FloatWritable(1);
        this.adjList = new ArrayWritable();
        this.flag = new BooleanWritable(true);
    }

    public void set (FloatWritable distance, ArrayWritable adjList, BooleanWritable flag){
        this.distance = distance;
        this.adjList = adjList;
        this.flag = flag;
    }

    public void setDistance(FloatWritable distance){
        this.distance = distance;
    }

    public void setAdjList(ArrayWritable adjList){
        this.adjList = adjList;
    }

    public void setFlag(BooleanWritable flag){
        this.flag = flag;
    }


    public FloatWritable getDistance() {
        return this.distance;
    }

    public ArrayWritable getAdjList() {
        return this.adjList;
    }

    public BooleanWritable getFlag() {
        return this.flag;
    }

    public void readFields(DataInput in) throws IOException {
        distance.readFields(in);
        adjList.readFields(in);
        flag.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        distance.write(out);
        adjList.write(out);
        flag.write(out);
    }

    public static PRNodeWritable read(DataInput in) throws IOException {
        PRNodeWritable pr = new PRNodeWritable();
        pr.readFields(in);
        return pr;
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        FloatWritable distance = this.distance;
        ArrayWritable adjList = this.adjList;
        BooleanWritable flag = this.flag;


        Object[] objects = adjList.toArray();
        long[] keys = new long[objects.length];
        System.arraycopy(objects, 0, keys, 0, objects.length);

        String s = new String(" ");
        for (long key : keys) {
            s = s + key.toString() + "," ;
        }
        s = s + " ";
        result.append( distance.toString() + " " + flag.toString() + s );
        return result.toString();

    }

    public String toString(LongWritable nid) {
        StringBuilder result = new StringBuilder();
        FloatWritable distance = this.distance;
        ArrayWritable adjList = this.adjList;
        BooleanWritable flag = this.flag;

        Object[] objects = adjList.toArray();
        long[] keys = new long[objects.length];
        System.arraycopy(objects, 0, keys, 0, objects.length);

        String s = new String(" ");
        for (long key : keys) {
            s = s + key.toString() + "," ;
        }
        s = s + " ";
        result.append( nid.toString() + "\t" + distance.toString() + " " + flag.toString() + s );
        return result.toString();

    }

    public void copy(PRNodeWritable pr, LongWritable nid){
        String prStr = pr.toString(nid);
        Text prText = new Text();
        prText.set(prStr);
        this.getByText(prText);
        return;
    }


    public static LongWritable[] getStringToArray(String str){
        String[] str1 = str.split(",");
        LongWritable[] array = new LongWritable[str1.length];

        for (int i = 0; i < str1.length; i++) {
            long l = Long.parseLong(str1[i]);
            LongWritable longWritable = new LongWritable(l);
            array[i] = longWritable;
        }
        return array;
    }


    public int getByText(Text t){
        PRNodeWritable node = new PRNodeWritable();
        String str = t.toString();
        String[] all = str.trim().split(" ");
        String[] nodeAndDist = all[0].split("\t");
        int nid = Integer.parseInt(nodeAndDist[0]);
        float distance = Float.parseFloat(nodeAndDist[1]);
        FloatWritable distanceWritable = new FloatWritable(distance);

        boolean flag = Boolean.parseBoolean(all[1]);
        BooleanWritable flagWritable = new BooleanWritable(flag);

        ArrayWritable arrayWritable = new ArrayWritable();
        if(all.length == 3)
        {
            LongWritable[] array = getStringToArray(all[2]);
            arrayWritable.set(array);
        }


        this.distance = distanceWritable;
        this.adjList = arrayWritable;
        this.flag = flagWritable;
        return nid;

    }


}
