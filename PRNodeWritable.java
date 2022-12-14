import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.HashMap;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Public
@InterfaceStability.Stable

public class PRNodeWritable implements Writable {
    // Some data
    private DoubleWritable distance = new DoubleWritable(1);
    private Text adjList = new Text();

    public BooleanWritable flag = new BooleanWritable(true);

    public void PRNodeWritable() throws IOException {
        this.distance = new DoubleWritable(1);
        this.adjList = new Text();
        this.flag = new BooleanWritable(true);
    }

    public void set (DoubleWritable distance, Text adjList, BooleanWritable flag){
        this.distance = distance;
        this.adjList = adjList;
        this.flag = flag;
    }

    public void setDistance(DoubleWritable distance){
        this.distance = distance;
    }

    public void setAdjList(Text adjList){
        this.adjList = adjList;
    }

    public void setFlag(BooleanWritable flag){
        this.flag = flag;
    }


    public DoubleWritable getDistance() {
        return this.distance;
    }

    public Text getAdjList() {
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
        DoubleWritable distance = this.distance;
        Text adjList = this.adjList;
        BooleanWritable flag = this.flag;

        String s = new String(" ");
        s = s + adjList.toString();
        s = s + " ";
        result.append( distance.toString() + " " + flag.toString() + s );
        return result.toString();

    }

    public String toString(LongWritable nid) {
        StringBuilder result = new StringBuilder();
        DoubleWritable distance = this.distance;
        Text adjList = this.adjList;
        BooleanWritable flag = this.flag;

        String s = new String(" ");
        s = s + adjList.toString();
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

    public static long[] stringToArray(Text t){
        String str = t.toString();
        String[] str1 = str.split(",");
        long[] array = new long[str1.length];
        for(int i=0;i<str1.length;i++){
            array[i] = Long.parseLong(str1[i]);
        }
        return array;
    }

    public int getByText(Text t){
        PRNodeWritable node = new PRNodeWritable();
        String str = t.toString();
        String[] all = str.trim().split(" ");
        String[] nodeAndDist = all[0].split("\t");
        int nid = Integer.parseInt(nodeAndDist[0]);
        double distance = Double.parseDouble(nodeAndDist[1]);
        DoubleWritable distanceWritable = new DoubleWritable(distance);

        boolean flag = Boolean.parseBoolean(all[1]);
        BooleanWritable flagWritable = new BooleanWritable(flag);

        Text text = new Text();
        if(all.length == 3)
        {
            text.set(all[2]);
        }

        this.distance = distanceWritable;
        this.adjList = text;
        this.flag = flagWritable;
        return nid;

    }


}
