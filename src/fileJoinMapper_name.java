
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class fileJoinMapper_name extends Mapper<LongWritable,Text,Text,JoinWritable> {

	String inputFilename;

@Override
protected void setup(Context context)throws IOException,InterruptedException {
	FileSplit filesplit=(FileSplit) context.getInputSplit();	
	inputFilename=filesplit.getPath().getName();
	System.out.println(inputFilename);
}

public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException { 
	String tokens[]=value.toString().split(",");		
	if(tokens.length==2)
	{
		System.out.println(tokens[0]+" "+tokens[1]);	
		context.write(new Text(tokens[0]),new JoinWritable(tokens[1],inputFilename));	
	}
}
	
}
