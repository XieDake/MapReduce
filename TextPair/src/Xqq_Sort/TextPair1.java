package Xqq_Sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextPair1 implements WritableComparable<TextPair1>{
	//成员变量！
	private Text first;
	private Text second;
	//构造方法！
	//话说可以重载构造方法——使满足各种各样的初始化！
	public TextPair1(Text first,Text second){
		set(first, second);
	}
	//基本setter/getter
	public void set(Text first,Text second){
		this.first=first;
		this.second=second;
	}
	public Text getFirst(){
		return first;
	}
	public Text getSecond(){
		return second;
	}
	//要覆写几个方法！
	//WritableComparable接口的write/readFields（PS：其实是Writable接口的俩方法——实现序列化和反序列化！）
	public void write(DataOutput out)throws IOException{
		//靠Text对像自身来实现！
		first.write(out);
		second.write(out);
	}
	public void readFields(DataInput in)throws IOException{
		first.readFields(in);
		second.readFields(in);
	}
	//其次就是Comparable接口的compareTo方法！
	public int compareTo(TextPair1 a){
		int cmp=a.first.compareTo(first);
		if(cmp!=0){
			return cmp;
		}else{
			return a.second.compareTo(second);
		}
	}
	//还有就是Object的几个方法！
	public boolean equals(Object a){
		if(a instanceof TextPair1){
			TextPair1 tp=(TextPair1)a;
			return ((TextPair1) a).first.equals(first)&&((TextPair1)a).second.equals(second);
		}else{
			return false;
		}
	}
	public String toString(){
		return first+"/t"+second;
	}
	public int hashCode(){
		return first.hashCode()*163+second.hashCode();
	}
	/////////////////////////////////////////////////////
	//还可以进一步优化！
	//实现TextPair的一个RawComparator——实现序列化比较（不用反序列化后再比较！）
	//其实就是继承WritableComparator-并改写原始compare方法！
	//静态内部类来实现！
	public static class ComParator extends WritableComparator{
		//还是要借住Text的RawComparator方法来序列化比较！——直接使用Comparator（）
		public static Text.Comparator TEXT_COMPARATOR=new Text.Comparator();
		//构造方法！
		public ComParator(){
			super(TextPair1.class);
		}
		//覆写compare方法！
		public int compare(byte[]b1,int s1,int l1,byte[]b2,int s2,int l2){
			try {
				//先得到俩个TextPair的各自第一个Text的长度！=初始头长+数组长度！
				int firstL1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
				int firstL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
				//先比较第一个Text！
				int cmp=TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				if(cmp!=0){
					return cmp;
				}else{
					return TEXT_COMPARATOR.compare(b1, s1+firstL1, l1-firstL1, b2, s2+firstL2, l2-firstL2);
				}
			} catch (IOException e) {
				// TODO: handle exception
				throw new IllegalArgumentException();
			}
		}
	}
	//话说以上是基本的TextPair的比较——当然还有更加高级的——实现定制的comparator;
	//还是以静态内部类的形式给出！
	//其实还是覆写WritableComparator的compare（），同时还实现compare的重载！
	public static class FirstComparator extends WritableComparator{
		public static Text.Comparator fIRST_Comparator=new Text.Comparator();
		//构造方法！
		public FirstComparator(){
			super(TextPair1.class);
		}
		//覆写&&重载compare（）！
		public int compare(byte[]b1,int s1,int l1,byte[]b2,int s2,int l2){
			try {
				//
				int firstL1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1, s1);
				int fisrtL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2, s2);
				//只比较第一个！
				return fIRST_Comparator.compare(b1, s1, firstL1, b2, s2, fisrtL2);
			} catch (IOException e) {
				// TODO: handle exception
				throw new IllegalArgumentException();
			}
		}
	}
}
