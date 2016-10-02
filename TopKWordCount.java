import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKWordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
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

  static <K,V extends Comparable<? super V>>
  SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
    SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
      new Comparator<Map.Entry<K,V>>() {
        @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
          int res = e2.getValue().compareTo(e1.getValue());
          return res != 0 ? res : 1;
        }
      }
    );
    sortedEntries.addAll(map.entrySet());
    return sortedEntries;
  }

  // Create your 2nd mapper here
  public static class TopKIdMapper
          extends Mapper<Object, Text, Text, Text>{

    private Text word_freq = new Text();
    private Text k    = new Text("-");

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      // Complete your 2nd mapper

      word_freq.set(value.toString());

      context.write(k,word_freq);
    }
  }


  // Create your 2nd reducer here
  public static class TopKReducer
          extends Reducer<Text,Text,Text,Text> {

    Map<String,Integer> sortedMap= new HashMap<String,Integer>();
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

      for (Text val : values) {
        String [] kword =val.toString().split("\t");
        sortedMap.put(kword[0],Integer.valueOf(kword[1]));
      }
      //sort
      SortedSet<Map.Entry<String,Integer>> ts=entriesSortedByValues(sortedMap);

      //output top K result
      int count = 0;
      int TopNum = Integer.valueOf(context.getConfiguration().get("numResults"));

      Iterator iterator = ts.iterator();
      while(iterator.hasNext()){
        String [] words =iterator.next().toString().split("=");
        result.set(words[0]+"\t"+words[1]);
        context.write(key,result);
        count++;
        if(count==TopNum){
          break;
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TopKWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);


    Configuration conf2 = new Configuration();
    conf2.set("numResults", args[3]);

    Job job2 = Job.getInstance(conf2, "TopKword count");

    // Insert your Code Here
    job2.setJarByClass(TopKWordCount.class);
    job2.setMapperClass(TopKWordCount.TopKIdMapper.class);
    job2.setReducerClass(TopKWordCount.TopKReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);


    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }
}

