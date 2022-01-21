import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JoinWritable implements Writable{
private Text mrValue;
private Text mrFilename;
	
public  JoinWritable() {
set(new Text(),new Text());
}	
public  JoinWritable(Text mrFilename,Text mrValue) {
set(mrFilename,mrValue);
}
public  JoinWritable(String mrFilename,String mrValue) {
	set(new Text(mrFilename),new Text(mrValue));
}
	@Override
	public void write(DataOutput out) throws IOException {

		mrFilename.write(out);
		mrValue.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		mrFilename.readFields(in);
		mrValue.readFields(in);
	}

	public Text getMrFilename() {
		return mrFilename;
	}

	public void set(Text mrFilename,Text mrValue) {
		this.mrFilename = mrFilename;
		this.mrValue = mrValue;

	}

	public Text getMrValue() {
		return mrValue;
	}

	@Override
	public int hashCode() {
		return mrValue.hashCode()*163+mrFilename.hashCode();
	}
	/*
	 * @Override public void write(DataOutput arg0) throws IOException { // TODO
	 * Auto-generated method stub
	 * 
	 * }
	 */
	

}
