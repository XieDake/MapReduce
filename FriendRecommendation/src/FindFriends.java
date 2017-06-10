import java.io.IOException;  
import java.net.URI;  
import java.net.URISyntaxException;  
import java.util.HashSet;  
import java.util.Iterator;  
import java.util.Set;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  

public class FindFriends {  
    public static class FindFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {  
        @Override  
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)  
                throws IOException, InterruptedException {  
            String line = value.toString();  
            String array[] = line.split("\t");  
            context.write(new Text(array[0]), new Text(array[1]));  
            context.write(new Text(array[1]), new Text(array[0]));  
        }  
    }  
      
    
      
    // Reducing进行笛卡尔乘积计算  
    public static class FindFriendsReduce extends Reducer<Text, Text, Text, Text> {  
        @Override  
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)  
                throws IOException, InterruptedException {  
            // 去重  
            Set<String> set = new HashSet<String>();  
            for (Text v : values) {  
                set.add(v.toString());  
            }  
              
            if (set.size() > 1) {  
                for (Iterator<String> i = set.iterator(); i.hasNext();) {  
                    String qqName = i.next();  
                    for (Iterator<String> j = set.iterator(); j.hasNext();) {  
                        String otherQqName = j.next();  
                        if (!qqName.equals(otherQqName)) {  
                            context.write(new Text(qqName), new Text(otherQqName));  
                        }  
                    }  
                }  
            }  
        }  
    }  
      
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {  
        final String INPUT_PATH = "hdfs://localhost:9000/user/hadoop/input/FriendRec/friend.txt";  
        final String OUTPUT_PATH = "hdfs://localhost:9000/user/hadoop/output/t1"; 
        Configuration conf = new Configuration();  
          
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);  
        if(fileSystem.exists(new Path(OUTPUT_PATH))) {  
            fileSystem.delete(new Path(OUTPUT_PATH), true);  
        }  
          
        Job job = Job.getInstance(conf, "FindFriends"); //设置一个用户定义的job名称  
        job.setJarByClass(FindFriends.class);  
        job.setMapperClass(FindFriendsMapper.class); //为job设置Mapper类  
//      job.setCombinerClass(IntSumReducer.class);    //为job设置Combiner类  
        job.setReducerClass(FindFriendsReduce.class); //为job设置Reducer类  
        job.setOutputKeyClass(Text.class);        //为job的输出数据设置Key类  
        job.setOutputValueClass(Text.class);    //为job输出设置value类  
          
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));  
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));  
  
        System.exit(job.waitForCompletion(true) ?0 : 1);        //运行job  
    }  
  
}  