import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class filejoinMapper_dept extends Mapper<LongWritable,Text,Text,Text> {

	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException { 
		String[] tokens=value.toString().split(",");		
		if(tokens.length>=2)
		{
		context.write(new Text(tokens[0]),new Text(tokens[1]));	
		}
	}

}
