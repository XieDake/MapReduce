import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class ReduceJoin {
	public static class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, Text>.Context context)throws IOException,InterruptedException{
			FileSplit fileSplit=(FileSplit)context.getInputSplit();
			String path=fileSplit.getPath().toString();
			//
			String line=value.toString();
			//
			if(line==null||line.equals("")){
				return;
			}
			//处理tb_a的记录！
			if(path.contains("tb_b")){
				String[]values=line.split("\t");
				if(values.length<3){
					return;
				}
				String id=values[0];
				String statyear=values[1];
				String num=values[2];
				//
				context.write(new Text(id), new Text("b#"+statyear+" "+num));
			}else if(path.contains("tb_a")){
				String[]values=line.split("\t");
				if(values.length<2){
					return;
				}
				String id=values[0];
				String name=values[1];
				//
				context.write(new Text(id), new Text("a#"+name));
			}
		}
	}
	//
	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key,Iterable<Text>values,Reducer<Text, Text, Text, Text>.Context context)throws IOException,InterruptedException{
			Vector<String>vectorA=new Vector<String>();
			Vector<String>vectorB=new Vector<String>();
			//
			for(Text val:values){
				if(val.toString().startsWith("a#")){
					vectorA.add(val.toString().substring(2));
				}else if (val.toString().startsWith("b#")) {
					vectorB.add(val.toString().substring(2));
				}
			}
			//
			int sizeA=vectorA.size();
			int sizeB=vectorB.size();
			//
			for(int i=0;i<sizeA;i++){
				for(int j=0;j<sizeB;j++){
					context.write(key, new Text(" "+vectorA.get(i)+" "+vectorB.get(j)));
				}
			}
		}
	}
	//
	private static final String INPUT_PATH="hdfs://localhost:9000/user/hadoop/input/input_join";
	private static final String OUTPUT_PATH="hdfs://localhost:9000/user/hadoop/output/join_outPut";
	//
	public static void main(String args[])throws IOException,InterruptedException,ClassNotFoundException{
		Configuration conf=new Configuration();
		//
		Job job=new Job(conf,ReduceJoin.class.getName());
		//设置自定义Mapper类和map（）的输出key/value类型！
		job.setMapperClass(ReduceJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置自定义Reducer类和reduce（）的输出key/value类型！
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//
		//设置输入目录和输入数据格式！
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		//设置输出目录和输出数据格式！
		FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		//
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
