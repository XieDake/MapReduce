import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class Emp_Dep implements WritableComparable{
	private String Name="";
	private String Sex="";
	private int Age=0;
	private int DepNo=0;
	private String DepName="";
	private String table="";
	public Emp_Dep(){}
	//getter
	public String getName(){
		return Name;
	}
	public String getSex(){
		return Sex;
	}
	public int getAge(){
		return Age;
	}
	public int getDepNo(){
		return DepNo;
	}
	public String getDepName(){
		return DepName;
	}
	public String getTable(){
		return table;
	}
	//setter
	public void setName(String name){
		this.Name=name;
	}
	public void setSex(String sex){
		this.Sex=sex;
	}
	public void setAge(int age){
		this.Age=age;
	}
	public void setDepNo(int DepNo){
		this.DepNo=DepNo;
	}
	public void setDepName(String DepName){
		this.DepName=DepName;
	}
	public void setTable(String table){
		this.table=table;
	}
	//
	public void write(DataOutput out)throws IOException{
		out.writeUTF(Name);
		out.writeUTF(Sex);
		out.writeInt(Age);
		out.writeInt(DepNo);
		out.writeUTF(DepName);
		out.writeUTF(table);
	}
	//
	public void readFields(DataInput in)throws IOException{
		this.Name=in.readUTF();
		this.Sex=in.readUTF();
		this.Age=in.readInt();
		this.DepNo=in.readInt();
		this.DepName=in.readUTF();
		this.table=in.readUTF();
	}
	//
	public int compareTo(Object o){
		return 0;
	}
	//
	public String toString(){
		return "EmpJoinDep [Name=" + Name + ", Sex=" + Sex + ", Age=" + Age
                + ", DepName=" + DepName + "]";
	}
}
