import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
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
