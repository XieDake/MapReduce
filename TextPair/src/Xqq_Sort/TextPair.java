package Xqq_Sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair>{
	//成员变量！
	private Text first;
	private Text second;
	//构造方法！
	//多种形式的初始化！
	public TextPair(){
		set(new Text(), new Text());
	}
	public TextPair(String first,String second){
		set(new Text(first),new Text(second));
	}
	public TextPair(Text first,Text second){
		set(first, second);
	}
	//成员变量是private的，所以需要一个public的setter和getter！
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
	//需要在这个子类中重写（override）几个方法！
	//Writable:定义了两个方法
	//1：将状态写到DataOutPut二进制流中;DataOutput接口实现输入的序列化！
	//2：从DataInput二进制流中读取状态！DataInput接口可对输入实现反序列化！
	public void write(DataOutput out)throws IOException{
		first.write(out);//first：序列化输出！
		second.write(out);//second:序列化输出！
	}
	public void readFields(DataInput in)throws IOException{
		first.readFields(in);//first二进制反序列化输入！
		second.readFields(in);//second二进制反序列化输入！
	}
	//由于map阶段要分配reducer，需要按键来计算Partition;
	public int hashCode(){
		return first.hashCode()*163+second.hashCode();//组合键是<first,second>,所以要靠这两个来计算Partition
	}
	//需要定义比较的几个方法！
	//1:内容是否想等！
	public boolean equals(Object o){
		if(o instanceof TextPair){
			TextPair temp=(TextPair)o;//向上转型！
			return first.equals(temp.first)&&second.equals(temp.second);
		}else {
			return false;
		}
	}
	//2:比较大小!
	public int compareTo(TextPair tp){
		//与tp比较大小！
		//先比较first，再比较second！
		//如果first能够比较出大小！则返回first的比较结果！
		int cmp=first.compareTo(tp.first);
		if(cmp!=0){
			return cmp;
		}
		//如果first不能比较出大小！则使用色彩哦你的进行比较！
		return second.compareTo(tp.second);
	}
	//
	public String toString(){
		return first+"\t"+second;
	}
	//用于TexPair的比较的RawComparator！不用反序列化就可以比较TextPair！
	//实际上不是直接实现RawComparator而是继承WritableComparator类！
	//使用WritableComparator提供的一些好的方法！
	//以静态内部类的形式来实现WritableComparator！
	public static class Comparator extends WritableComparator{
		//需要使用Text对像本身实现好的RawComparator，来进行序列化比较！
		private static final Text.Comparator TEXT_COMPARATOR=new Text.Comparator();
		public Comparator(){
			super(TextPair.class);
		}
		public int compare(byte[]b1,int s1,int l1,byte[]b2,int s2,int l2){
			try{
				//计算两个TextPair的第一个字符串长度！
                /** 
                 * 由于是Text类型，Text是标准的UTF-8字节流， 
                 * 由一个变长整形开头表示Text中文本所需要的长度，接下来就是文本本身的字节数组 
                 * decodeVIntSize返回变长整形的长度，readVInt表示文本字节数组的长度，加起来就是第一个成员Text的长度 
                 */  
				int firstL1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1, s1);
				int firstL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2, s2);
				int cmp=TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				if (cmp!=0) {
					return cmp;
				}
				//如果第一个字符串比较不能区分大小！比较第二个字符串！
				return TEXT_COMPARATOR.compare(b1, s1+firstL1, l1-firstL1, b2, s2+firstL2, l2-firstL2);
			}catch(IOException e){
				throw new IllegalArgumentException();
			}
		}
		}
	//还是以静态内部类的形式来实现！
	//只比较第一个Text即可！
	public static class FirstComparator extends WritableComparator{
		//需要使用Text对像本身实现好的RawComparator，来进行序列化比较！
				private static final Text.Comparator TEXT_COMPARATOR=new Text.Comparator();
				public FirstComparator(){
					super(TextPair.class);
				}
				//覆写FirstComparator的compare方法！
				public int compare(byte[]b1,int s1,int l1,byte[]b2,int s2,int l2){
					try{
						//计算两个TextPair的第一个字符串长度！
		                /** 
		                 * 由于是Text类型，Text是标准的UTF-8字节流， 
		                 * 由一个变长整形开头表示Text中文本所需要的长度，接下来就是文本本身的字节数组 
		                 * decodeVIntSize返回变长整形的长度，readVInt表示文本字节数组的长度，加起来就是第一个成员Text的长度 
		                 */  
						int firstL1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1, s1);
						int firstL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2, s2);
						return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
					}catch(IOException e){
						throw new IllegalArgumentException();
					}
				}
				//子类中不光覆写FirstComparator的compare方法！还实现了重载compare方法！
				//直接对像比较！
				public int compare(WritableComparable a,WritableComparable b){
					if(a instanceof TextPair&& b instanceof TextPair){
						return((TextPair)a).first.compareTo(((TextPair)b).first);
					}
					return super.compare(a, b);
				}
	}
}
