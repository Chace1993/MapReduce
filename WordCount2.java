import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    IntWritable word_freq = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      HashMap<Text, IntWritable> hashMap =new HashMap<>();
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        Text word =new Text();
        word.set(itr.nextToken());

        word_freq=hashMap.get(word);
        //local data reduction,using hashmap
        if(word_freq==null){
          word_freq=new IntWritable(1);
        }else{
          word_freq=new IntWritable(word_freq.get()+1);
        }
        hashMap.put(word,word_freq);
      }
      //after combine in map ,then output the result
      for(Map.Entry<Text,IntWritable> map : hashMap.entrySet()){
        context.write(map.getKey(),map.getValue());
      }


    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String temp_path = "temp";

    Job job = Job.getInstance(conf, "word count using hash");

    job.setJarByClass(WordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

 
}
