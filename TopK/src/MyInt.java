/**
 * 
 * @author hadoop
 * 实现自己的int的封装！
 */
public class MyInt {
	private Integer value;
	//
	public MyInt(){
		this.value=0;
	}
	public MyInt(Integer value){
		setValue(value);
	}
	//getter
	public Integer getValue(){
		return value;
	}
	//setter
	public void setValue(Integer value){
		this.value=value;
	}
	//
	public int compareTo(MyInt o){
		return value.compareTo(o.getValue());
	}
}
