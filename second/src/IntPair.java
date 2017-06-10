import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.sun.org.apache.bcel.internal.generic.RETURN;

public class IntPair implements WritableComparable<IntPair>{
	//数据成员！
	private int first;
	private int second;
	//构造方法！
	public IntPair(int first,int second){
		set(first, second);
	}
	public IntPair(){
		set(0, 0);
	}
	//getter
	public int getFirst(){
		return first;
	}
	public int getSecond(){
		return second;
	}
	//setter
	public void set(int first,int second){
		this.first=first;
		this.second=second;
	}
	//覆写几个方法！
	//序列化——write()!
	public void write(DataOutput out)throws IOException{
		out.writeInt(first);
		out.writeInt(second);
	}
	//反序列化——readFields()!
	public void readFields(DataInput in)throws IOException{
		first=in.readInt();
		second=in.readInt();
	}
	//compareTo
	public int compareTo(IntPair o){
		if(first!=o.first){
			return first<o.first?-1:1;
		}else if(second!=o.second){
			return second<o.second?-1:1;
		}else{
			return 0;
		}
	}
	//Object的几个方法！
	//hashCode!
	public int hashCode(){
		return first*157+second;
	}
	//equals
	public boolean equals(Object o){
		if(o==null){
			return false;
		}
		if(this==o){
			return true;
		}
		if(o instanceof IntPair){
			IntPair tmp=(IntPair)o;
			return first==tmp.first&&second==tmp.second;
		}else{
			return false;
		}
	}
}