import java.io.IOException;

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


public class Top1 {
	//Mapper
	public static class Top1Mapper extends Mapper<LongWritable, Text, NullWritable, LongWritable>{
		long max=Long.MIN_VALUE;
		//
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, NullWritable, LongWritable>.Context context)throws IOException,InterruptedException{
			long tmp=Long.parseLong(value.toString().trim());
			//
			if(tmp>max){
				max=tmp;
			}
		}
		//
		protected void cleanup(Mapper<LongWritable, Text, NullWritable, LongWritable>.Context context)throws IOException,InterruptedException{
			context.write(NullWritable.get(), new LongWritable(max));
		}
	}
	//Reducer
	public static class Top1Reducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable>{
		long max=Long.MIN_VALUE;
		//
		protected void reduce(NullWritable key,Iterable<LongWritable>values,Reducer<NullWritable, LongWritable, NullWritable, LongWritable>.Context context)throws IOException,InterruptedException{
			for(LongWritable val:values){
				if(val.get()>max){
					max=val.get();
				}
			}
			//
			context.write(NullWritable.get(), new LongWritable(max));
		}
	}
	//配置！
	public static void main(String[]args)throws IOException,InterruptedException,ClassNotFoundException{
		//
		Configuration conf=new Configuration();
		//
		Job job=new Job(conf,Top1.class.getName());
		job.setJarByClass(Top1.class);
		//Mapper和map（）！
		job.setMapperClass(Top1Mapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		//Reducer和reduce()！
		job.setReducerClass(Top1Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(LongWritable.class);
		//
		job.setNumReduceTasks(1);
		//
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		//
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		//
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
