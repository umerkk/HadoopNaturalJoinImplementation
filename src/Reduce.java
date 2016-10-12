import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/*
 * **********************************************
 *                                              *
 * Comp 6521 - Advance Database Applications    *
 * Project # 1                                  *
 * Implement Natural Join using Hadoop          *
 *                                              *
 * Developed By:                                *
 * Muhammad Umer (40015021)                     *
 * Hamzah Hamdi                                 *
 *                                              *
 * **********************************************
 */

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	ArrayList < Text > RelS;
	ArrayList < Text > RelR; 

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		RelS = new ArrayList < Text >() ;
		RelR = new ArrayList < Text >() ;
		
		while (values.hasNext()) {
			String relationValue = values.next().toString();

			if (relationValue.indexOf('A') >= 0){
				String finalVal = relationValue.substring(1, relationValue.length());
				RelR.add(new Text(finalVal));
			} else {
				String finalVal = relationValue.substring(1, relationValue.length());
				RelS.add(new Text(finalVal));
			}
		}

		for( Text r : RelR ) {
			for (Text s : RelS) {
				output.collect(new Text(), new Text(r + "," + key.toString() + "," + s));
			}
		}
	}

}
