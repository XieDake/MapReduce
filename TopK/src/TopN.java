import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TopN {
	//自定义Mapper类！
	public static class TopMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable>{
		//用TreeMap存此map任务的topK！原因：保证顺序/log（n）
		private TreeMap<Long, Long>tree=new TreeMap<Long,Long>();
		public static final int K=100;
		//
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, NullWritable, LongWritable>.Context context)throws IOException,InterruptedException{
			long temp=Long.parseLong(value.toString().trim());
			//注意TreeMap存的时候key/value一样即可！
			tree.put(temp, temp);
			//维护size=K！
			if(tree.size()>K){
				tree.remove(tree.firstKey());
			}
		}
		//map任务执行完毕！treeMap存储topK！
		protected void cleanup(Mapper<LongWritable,Text, NullWritable,LongWritable>.Context context)throws IOException,InterruptedException{
			for(Long val:tree.values()){
				context.write(NullWritable.get(), new LongWritable(val));
			}
		}
	}
	//自定义Reducer！
	public static class TopReducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable>{
		//最后使用一个reduce即可！
		//同样最后使用一个TreeMap来存最后结果！
		private TreeMap<Long, Long>tree=new TreeMap<Long,Long>();
		public static final int K=100;
		//默认输入数据没有重复！
		protected void reduce(NullWritable key,Iterable<LongWritable>values,Reducer<NullWritable, LongWritable, NullWritable, LongWritable>.Context context)throws IOException,InterruptedException{
			for(LongWritable val:values){
				tree.put(val.get(), val.get());
				if(tree.size()>K){
					tree.remove(tree.firstKey());
				}
			}
			//循环结束就是存好最终的topK咯！
			for(Long val:tree.descendingKeySet()){
				context.write(NullWritable.get(),new LongWritable(val));
			}
		}
	}
	//配置输出！
	public static void main(String[]args)throws IOException,InterruptedException,ClassNotFoundException{
		//conf
		Configuration conf=new Configuration();
		//job
		Job job=new Job(conf,TopN.class.getName());
		job.setJarByClass(TopN.class);
		//Mapper和map
		job.setMapperClass(TopMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		//Reducer和reduce！
		job.setReducerClass(TopReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(LongWritable.class);
		//
		//
		job.setNumReduceTasks(1);
		//
		FileInputFormat.addInputPath(job,new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		//
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		//
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
