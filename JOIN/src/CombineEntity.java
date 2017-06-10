import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

//这是为了方便join输出定义的——输出实体！
public class CombineEntity {
	private Text joinKey;
	private Text flag;
	private Text secondPart;//key意外其余部分！
	//getter
	public Text getJoinKey(){
		return joinKey;
	}
	public Text getFlag(){
		return flag;
	}
	public Text getSecondPart(){
		return secondPart;
	}
	//setter
	public void setJoinKey(Text joinKey){
		this.joinKey=joinKey;
	}
	public void setFlag(Text flag){
		this.flag=flag;
	}
	public void setSecondPart(Text secondPart){
		this.secondPart=secondPart;
	}
	//
	public void write(DataOutput out)throws IOException{
		joinKey.write(out);
		flag.write(out);
		secondPart.write(out);
	}
	//
	public void readFields(DataInput in)throws IOException{
		joinKey.readFields(in);
		flag.readFields(in);
		secondPart.readFields(in);
	}
	//
	public int compareTo(CombineEntity o){
		return this.joinKey.compareTo(o.joinKey);
	}
}
