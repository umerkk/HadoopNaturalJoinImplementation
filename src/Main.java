import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.FileSystemCounter;
public class Main {

    public static class FirstMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            Text a = new Text();
            Text b = new Text();

            a.set(tokenizer.nextToken());
            b.set(tokenizer.nextToken());
            Text relation = new Text("R"+a.toString());

            output.collect(b, relation);
        }
    }

    public static class SecondMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            Text b = new Text();
            Text c = new Text();

            b.set(tokenizer.nextToken());
            c.set(tokenizer.nextToken());

            Text relation = new Text("S"+c.toString());

            output.collect(b, relation);

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            ArrayList < Text > RelationS = new ArrayList < Text >() ;
            ArrayList < Text > RelationR = new ArrayList < Text >() ;

            while (values.hasNext()) {
                String relationValue = values.next().toString();
                if (relationValue.indexOf('R') >= 0){
                    RelationR.add(new Text(relationValue));
                } else {
                    RelationS.add(new Text(relationValue));
                }
            }

            for( Text r : RelationR ) {
                for (Text s : RelationS) {
                    output.collect(key, new Text(r + "," + key.toString() + "," + s));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MultipleInputs.class);
        conf.setJobName("TwoWayJoin");

        //conf.setCombinerClass(Main.Reduce.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //conf.setCombinerClass(Main.Reduce.class);
        conf.setReducerClass(Main.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

	     
        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Main.FirstMap.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Main.SecondMap.class);
        
        //conf.setCombinerClass(Main.Reduce.class);


        Path output = new Path(args[2]); 

        FileOutputFormat.setOutputPath(conf, output);


        JobClient.runJob(conf);

    }

}
