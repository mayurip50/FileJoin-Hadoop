

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;;

public class fileJoinReducer extends Reducer<Text,JoinWritable,NullWritable,Text> {
		String name=null;
		String dept=null;
	public void reduce(Text key,Iterable<JoinWritable> value,Context context)throws IOException,InterruptedException {
		/*	StringBuffer id=new StringBuffer(key.toString()).append(",");
	int cnt=0;
		
		 * for(Text val : value) {
		 * 
		 * id.append(val.toString()); if(cnt<1) { id.append(","); } cnt++;
		 * 
		 * } context.write(NullWritable.get(),new Text(id.toString()));
		 */

		for(JoinWritable val : value) {
			if(val.getMrFilename().toString().equals("file1.txt")) {
				name=val.getMrValue().toString();
			}
			else if(val.getMrFilename().toString().equals("file2.txt")) {
				dept=val.getMrValue().toString();
			}
		}
		StringBuffer id=new StringBuffer(key.toString()).append(",");
		id.append(name).append(",").append(dept);
		context.write(NullWritable.get(), new Text(id.toString()));
	}
}
