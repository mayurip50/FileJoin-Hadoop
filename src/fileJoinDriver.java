
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class fileJoinDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
Configuration conf=new Configuration();
String[] Otherargs=new GenericOptionsParser(conf,args).getRemainingArgs();
		
		 if(Otherargs.length!=3) {
		 System.err.println("Usage : MR Job<in> [in..]<out>"); System.exit(2); }
		
Job job=new Job(conf,"MR fileJoin Job");
job.setJarByClass(fileJoinDriver.class);
job.setMapperClass(fileJoinMapper_name.class);
//job.setCombinerClass(fileJoinReducer.class);
job.setReducerClass(fileJoinReducer.class);

job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(JoinWritable.class);

job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);

		/*
		 * for(int i=0;i<Otherargs.length-1;i++) { FileInputFormat.addInputPath(job, new
		 * Path(Otherargs[i])); }
		 */
FileInputFormat.addInputPath(job, new Path(Otherargs[0]));
FileInputFormat.addInputPath(job, new Path(Otherargs[1]));
FileOutputFormat.setOutputPath(job, new Path(Otherargs[2]));
System.exit(job.waitForCompletion(true)? 0: 1);	
	
	}

}
