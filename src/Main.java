import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

/*
 * **********************************************
 * 												*
 * 	Comp 6521 - Advance Database Applications	*
 * 				Project # 1						*
 * 		Implement Natural Join using Hadoop		*
 * 												*
 * 				Developed By:					*
 * 			Muhammad Umer (40015021)			*
 * 				Hamzah Hamdi					*
 * 												*
 * **********************************************
 */

public class Main {

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MultipleInputs.class);
        conf.setJobName("Comp 6521 Project 1 - Natural Join");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
	     
        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, RMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, SMapper.class);

        FileOutputFormat.setOutputPath(conf, new Path(args[2]));


        JobClient.runJob(conf);

    }

}
