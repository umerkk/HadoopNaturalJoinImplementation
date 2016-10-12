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



    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MultipleInputs.class);
        conf.setJobName("TwoWayJoin");

        //conf.setCombinerClass(Main.Reduce.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //conf.setCombinerClass(Main.Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

	     
        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, RMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, SMapper.class);
        
        //conf.setCombinerClass(Main.Reduce.class);


        Path output = new Path(args[2]); 

        FileOutputFormat.setOutputPath(conf, output);


        JobClient.runJob(conf);

    }

}
