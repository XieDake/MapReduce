import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;
public class SemiJoin {
	//自定义Mapper
	protected class SemiJoinMapper extends Mapper<LongWritable, Text,Text,CombineEntity>{
		//
		private CombineEntity combine=new CombineEntity();
		private Text joinKey=new Text();
		private Text flag=new Text();
		private Text secondPart=new Text();
		//
		//HashSet来实现Set村小表的key！
		HashSet<String>joinKeySet=new HashSet<String>();
		//setUp_先从node中读取小表！
		protected void setup(Mapper<LongWritable, Text, Text, CombineEntity>.Context context)throws IOException{
			//
			BufferedReader br=null;
			String temp="";
			//获得文件名
			Path[]paths=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			//小表的文件名“a.txt”
			for(Path path:paths){
				if (path.getName().contains("a.txt")) {
					//创建读取文件流！
					br=new BufferedReader(new FileReader(path.toString()));
					//
					while((temp=br.readLine())!=null){
						//
						String[]splits=temp.split(",");
						//
						joinKeySet.add(splits[0]);
					}
				}
			}
		}
		//map()函数！——两个功能1：标记数据来源——2：过滤大表数据！
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, CombineEntity>.Context context)throws IOException,InterruptedException{
			//按照文件名来进行tag！
			String pathName=((FileSplit)(context.getInputSplit())).getPath().toString();
			//
			if (pathName.contains("a.txt")) {
				//对小表数据进行操作！
				String[]valuesTemp=value.toString().split(",");
				//也过滤！
				if(joinKeySet.contains(valuesTemp[0])){
					joinKey.set(valuesTemp[0]);
					flag.set("0");
					secondPart.set(valuesTemp[1]+"\t"+valuesTemp[2]);
					//
					combine.setFlag(flag);
					combine.setJoinKey(joinKey);
					combine.setSecondPart(secondPart);
					//表1——写出！
					context.write(combine.getJoinKey(), combine);
				}else {
					//过滤！
					return;
				}
			}else if(pathName.contains("b.txt")){
				String[]valuesTemp=value.toString().split(",");
				if(joinKeySet.contains(valuesTemp[0])){
					flag.set("1");
					joinKey.set(valuesTemp[0]);
					secondPart.set(valuesTemp[1]+"\t"+valuesTemp[2]);
					//
					combine.setFlag(flag);
					combine.setJoinKey(joinKey);
					combine.setSecondPart(secondPart);
					//
					context.write(combine.getJoinKey(), combine);
				}else {
					//过滤！
					return;
				}
			}
		}
	}
	//自定义Reducer类！——实现reduce端join！
	protected class SemiJoinReducer extends Reducer<Text, CombineEntity, Text, Text>{
		//reduce（）函数！
		protected void reduce(Text key,Iterable<CombineEntity>values,Reducer<Text, CombineEntity, Text, Text>.Context context)throws IOException,InterruptedException{
			Text outPut=new Text();
			ArrayList<Text>table1=new ArrayList<Text>();
			ArrayList<Text>table2=new ArrayList<Text>();
			//
			for(CombineEntity val:values){
				if(val.getFlag().toString().trim().equals("0")){
					table1.add(val.getSecondPart());
				}else if(val.getFlag().toString().trim().equals("1")) {
					table2.add(val.getSecondPart());
				}
			}
			//笛卡尔积！
			for(Text val1:table1){
				for(Text val2:table2){
					outPut.set(val1+"\t"+val2);
					//写出去！
					context.write(key, outPut);
				}
			}
		}
	}
	//配置
	private static String INPUT_PATH1="";//小表——在cache中的！
	private static String INPUT_PATH2="";//大表——在HDFS中的！
	private static String OUTPUT_PATH="";//输出表！
	//
	public static void main(String[]args)throws IOException,InterruptedException,ClassNotFoundException,URISyntaxException{
		//
		Configuration conf=new Configuration();
		//获得命令行参数！
		String[]otherArgs=new GenericOptionsParser(conf, args).getRemainingArgs();
		//给路径赋值！
		INPUT_PATH1=otherArgs[0];
		INPUT_PATH2=otherArgs[1];
		OUTPUT_PATH=otherArgs[2];
		//
		//创建job！
		Job job=new Job(conf,SemiJoin.class.getName());
		//打jar包
		job.setJarByClass(MapJoin.class);
		//
		//自定义Mapper配置和map输出key/value格式类型！
		job.setMapperClass(SemiJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CombineEntity.class);
		//
		//自定义Reducer配置和reduce输出key/value格式类型！
		job.setReducerClass(SemiJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//设置大表的输入目录和输入数据格式！
		FileInputFormat.setInputPaths(job, INPUT_PATH2);
		job.setInputFormatClass(TextInputFormat.class);
		//设置输出目录和输出数据格式！
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		//提交job之前——小表分配到各个node上！
		DistributedCache.addCacheFile(new URI(INPUT_PATH1), conf);
		//
		//
		//提交job！
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
