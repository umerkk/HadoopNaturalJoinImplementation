import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	
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
