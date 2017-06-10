import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapJoin {	
	//由于要输出多个字段——我们之间定义一种自己的类型——方便操作！
	//自定义Mapper类！
	public static class MapJoinMapper extends Mapper<LongWritable, Text, NullWritable, Emp_Dep>{
		private Map<Integer, String>joinData=new HashMap<Integer,String>();//用来存放小表的key/value！
		//为了保证效率——资源初始化操作——setUp完成！
		protected void setup(Mapper<LongWritable, Text, NullWritable, Emp_Dep>.Context context)throws IOException{
			//目的是将小表中数据——存在Map中！
			Path[]paths=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			//缓存一个文件——第一个吧!
			BufferedReader reader=new BufferedReader(new FileReader(paths[0].toString()));
			//
			String str=null;
			//从缓存中一行一行的存入Map中！
			try{
				while((str=reader.readLine())!=null){
					//按"\t"分割！
					String[]splits=str.split("\t");
					//存入Map中！
					joinData.put(Integer.parseInt(splits[0]), splits[1]);
				}
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				reader.close();
			}
		}
		//map()实现join即可！
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, NullWritable, Emp_Dep>.Context context)throws IOException,InterruptedException{
			//HDFS中取第二个表！_取出一行！
			String[]values=value.toString().split("\t");
			//
			Emp_Dep emp_Dep=new Emp_Dep();
			//
			emp_Dep.setName(values[0]);
			emp_Dep.setSex(values[1]);
			emp_Dep.setAge(Integer.parseInt(values[2]));
			//单独拿出大表的depNo——准备执行join！
			int depNo=Integer.parseInt(values[3]);
			//按照大表的depNo取出小表中depNo对应的depName！
			String depName=joinData.get(depNo);
			//join操作！将小表的depName写到Emp_DEP中！
			emp_Dep.setDepName(depName);
			//join结束！
			//map输出！
			context.write(NullWritable.get(), emp_Dep);
		}
	}
	//由于只是实现join操作_不用reduce，所以只需要map就能输出——这时候输出key为NullWritable类型！只需输出Value！value为我们自定义类型！
	//定义三个路经！
	private static  String INPUT_PATH1="";//th_a_HDFS中的大表！
	private static  String INPUT_PATH2="";//th_b_缓存中的小表！
	private static  String OUT_PATH="";//输出目录！
	//配置！
	public static void main(String[]args)throws IOException,InterruptedException,ClassNotFoundException{
		//创建配置信息！
		Configuration conf=new Configuration();
		//获得命令行参数！
		String[]otherArgs=new GenericOptionsParser(conf, args).getRemainingArgs();
		//给路径赋值！
		INPUT_PATH1=otherArgs[0];
		INPUT_PATH2=otherArgs[1];
		OUT_PATH=otherArgs[2];
		//
		//创建job！
		Job job=new Job(conf,MapJoin.class.getName());
		//打jar包
		job.setJarByClass(MapJoin.class);
		//
		//自定义Mapper配置和map输出key/value格式类型！
		job.setMapperClass(MapJoinMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Emp_Dep.class);
		//没有Reducer
		//设置HDFS大表输入目录和输入数据格式！
		FileInputFormat.setInputPaths(job, INPUT_PATH1);
		job.setInputFormatClass(TextInputFormat.class);
		//设置输出目录和输出数据格式！
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		//
		//在job提交之前设置缓存文件小表的配置！
		DistributedCache.addCacheFile(new Path(INPUT_PATH2).toUri(), conf);
		//提交job！
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
