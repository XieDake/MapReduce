import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
*这里使用MR的迭代模式！
*先运行第一个MR，第一个MR的输出是第二个的输入！
*所以第一个先是WordCount运行程序——再一个就是TopK运行程序！
**/
public class WordTopK {
	//wordCount——Mapper！
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		//
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, IntWritable>.Context context)throws IOException,InterruptedException{
			String word=value.toString().trim();
			context.write(new Text(word), new IntWritable(1));
		}
	}
	//WordCount__Reducer!
	public static class Reducer1 extends Reducer<Text, IntWritable, NullWritable, Text>{
		protected void reduce(Text key,Iterable<IntWritable>values,Reducer<Text, IntWritable, NullWritable, Text>.Context context)throws IOException,InterruptedException{
			Integer count=0;
			for(IntWritable val:values){
				count+=val.get();
			}
			context.write(NullWritable.get(), new Text(key.toString().trim()+"\t"+count));
		}
	}
	//TopK__Mapper!——由于词频做key的话，TreeMap会覆盖——丢失数据！所以只能使用Map/reduce排序来取topK！
	public static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, Text>{
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, IntWritable, Text>.Context context)throws IOException,InterruptedException{
			//map的任务就是使词频做key，word做value！
			String[]line=value.toString().trim().split("\t");
			String word=line[0].toString();
			Integer count=Integer.parseInt(line[1]);
			//
			context.write(new IntWritable(count), new Text(word));
		}
	}
	//定义一个keyComparator——因为要实现词频的降序排列——所以需要重新写一个key比较器！
	public static class keyComparatorDense extends IntWritable.Comparator{
		public int compare(byte[]b1,int s1,int l1,byte[]b2,int s2,int l2){
			return -1*super.compare(b1, s1, l1, b2, s2, l2);
		}
	}
	//如果题目是返回词频最小的K个的话——不用该了！
	//TopK__Reducer!
	public static class Reducer2 extends Reducer<IntWritable, Text, Text, IntWritable>{
		//注意reduce数目是一个！
		//注意此时reduce的输入的key是排好序的！
		//所以只需要存K个单词即可！
		LinkedHashMap<String, Integer>maps=new LinkedHashMap<String,Integer>();
		public static int K=10;
		int times=0;//控制次数的！只需要存前k个即可！
		//
		protected void reduce(IntWritable count,Iterable<Text>words,Reducer<IntWritable,Text, Text, IntWritable>.Context context)throws IOException,InterruptedException{
			for(Text word:words){
				//一个count可能有好几个单词！——同频的单词！
				if(times<K){
					maps.put(word.toString(),count.get());
					times++;
				}
			}
		}
		//cleanup输出！
		protected void cleanup(Reducer<IntWritable, Text, Text, IntWritable>.Context context)throws IOException,InterruptedException{
			for(String word:maps.keySet()){
				context.write(new Text(word), new IntWritable(maps.get(word)));
			}
		}
	}
	//
	//配置！
	//文件路径！
	public static String INPUT="hdfs://localhost:9000/user/hadoop/input/Word";
	public static String OUTPUT1="hdfs://localhost:9000/user/hadoop/output/temp";
	public static String OUTPUT="hdfs://localhost:9000/user/hadoop/output/t1";
	public static void main(String[]args)throws IOException,InterruptedException,ClassNotFoundException{
		//################################################
		//conf1
		Configuration conf1=new Configuration();
		Job job1=new Job(conf1,WordTopK.class.getName());
		//Mapper/map
		job1.setMapperClass(Mapper1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		//Reducer/reduce
		job1.setReducerClass(Reducer1.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		//
		FileInputFormat.addInputPath(job1,new Path(INPUT));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT1));
		job1.waitForCompletion(true);
		//################################################
		//conf2!
		Configuration conf2=new Configuration();
		Job job2=new Job(conf2,WordTopK.class.getName());
		//Mapper/map
		job2.setMapperClass(Mapper2.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		//Reducer/reduce
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		//
		job2.setSortComparatorClass(keyComparatorDense.class);
		//
		FileInputFormat.addInputPath(job2,new Path(OUTPUT1));
		FileOutputFormat.setOutputPath(job2, new Path(OUTPUT));
		//
		System.exit(job2.waitForCompletion(true)?0:1);
	}
}
