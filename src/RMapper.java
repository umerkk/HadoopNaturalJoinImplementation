import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	 
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
