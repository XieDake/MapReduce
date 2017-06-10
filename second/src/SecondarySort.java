import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SecondarySort{
	//自定义组合键——intPair！
	public static class IntPair implements WritableComparable<IntPair>{
		//成员变量！
		private int first;
		private int second;
		//构造方法！
		public IntPair(int first,int second){
			set(first, second);
		}
		public IntPair(){
			set(0, 0);
		}
		//setter
		private void set(int first,int second){
			this.first=first;
			this.second=second;
		}
		//getter
		private int getFirst(){
			return first;
		}
		private int getSecond(){
			return second;
		}
		//覆写几个方法！
		//序列化——write
		public void write(DataOutput out)throws IOException{
			out.writeInt(first);
			out.writeInt(second);
		}
		//反序列化——readFields
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
		//Object类覆写两个方法！
		public boolean equals(Object o){
			if(o==null){
				return false;
			}
			if(this==o){
				return true;
			}
			if(o instanceof IntPair){
				IntPair tmp=(IntPair)o;
				return tmp.first==first&&tmp.second==second;
			}else{
				return false;
			}
		}
		public int hashCode(IntPair o){
			return o.first*157+second;
		}
		//
		//由于是新键——所以得实现三个类！
		//1：分区函数类——不能按新的组合键来分区——要按照第一个key来分区！
		public static class FirstPartitioner extends Partitioner<IntPair,IntWritable>{
			public int getPartition(IntPair key,IntWritable value,int numPartitions){
				//hash(key)%numPartitions
				return Math.abs(key.getFirst()*127)%numPartitions;
			}
		}
		//2:分组函数类！
		//由于是新键——所以——reduce不能按照新的组合键来分组——要按照第一个key进行分组！_其实就是比较第一个key！
		public static class GroupComparator extends WritableComparator{
			protected GroupComparator(){
				super(IntPair.class,true);
			}
			//两个WritableComparable比较！
			public int compare(WritableComparable w1,WritableComparable w2){
				IntPair p1=(IntPair)w1;
				IntPair p2=(IntPair)w2;
				//
				int n1=p1.getFirst();
				int n2=p2.getFirst();
				//
				return n1==n2?0:(n1<n2?-1:1);
			}
		}
		//3:话说还得实现组合key比较类！_这里我们没有实现——只靠intPair的compareTo方法就行！
		///////////////////////////////////////////////////////////////////////////////////////
		//功能类！
		//Map类！
		public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable>{
			//成员变量！_输出的key/value！
			private final IntPair keyOut=new IntPair();
			private final IntWritable valueOut=new IntWritable();
			//map函数！
			public void map(LongWritable keyIn,Text valueIn,Context context)throws IOException,InterruptedException{
				//首先解析valueIn;
				String line=valueIn.toString();
				StringTokenizer tokenizer=new StringTokenizer(line);
				//
				int first=0;
				int second=0;
				if(tokenizer.hasMoreTokens()){
					first=Integer.parseInt(tokenizer.nextToken());
					if(tokenizer.hasMoreTokens()){
						second=Integer.parseInt(tokenizer.nextToken());
					}
				}
				//
				keyOut.set(first, second);
				valueOut.set(second);
				//
				context.write(keyOut, valueOut);
			}
		}
		//Reduce类！
		public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable>{
			//成员变量——输出key/value！
			private final Text keyOut=new Text();
			//reduce函数！
			public void reduce(IntPair keyIn,Iterable<IntWritable>valuesIn,Context context)throws IOException,InterruptedException{
				//
				keyOut.set(Integer.toString(keyIn.getFirst()));
				//
				for(IntWritable value:valuesIn){
					context.write(keyOut, value);
				}
			}
		}
		//配置！
	    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	    {
	        // TODO Auto-generated method stub
	        // 读取hadoop配置
	        Configuration conf = new Configuration();
	        // 实例化一道作业
	        Job job = new Job(conf, "secondarysort");
	        job.setJarByClass(SecondarySort.class);
	        // Mapper类型
	        job.setMapperClass(Map.class);
	        // 不再需要Combiner类型，因为Combiner的输出类型<Text, IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
	        //job.setCombinerClass(Reduce.class);
	        // Reducer类型
	        job.setReducerClass(Reduce.class);
	        // 分区函数
	        job.setPartitionerClass(FirstPartitioner.class);
	        // 分组函数
	        job.setGroupingComparatorClass(GroupComparator.class);

	        // map 输出Key的类型
	        job.setMapOutputKeyClass(IntPair.class);
	        // map输出Value的类型
	        job.setMapOutputValueClass(IntWritable.class);
	        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
	        job.setOutputKeyClass(Text.class);
	        // rduce输出Value的类型
	        job.setOutputValueClass(IntWritable.class);

	        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
	        job.setInputFormatClass(TextInputFormat.class);
	        // 提供一个RecordWriter的实现，负责数据输出。
	        job.setOutputFormatClass(TextOutputFormat.class);

	        // 输入hdfs路径
	        FileInputFormat.setInputPaths(job, new Path(args[0]));
	        // 输出hdfs路径
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        // 提交job
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
		
	}
}