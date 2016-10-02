import java.io.File;
import java.io.IOException;
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

public class MaxWordCount {

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

    public static class IdMapper
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

    public static class IntMaxReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        private String max_word;

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Complete your 2nd reducer
            int word_freq,max_freq=0;

            for (Text val : values) {
                //split value
                String[] words = val.toString().split("\t");
                word_freq=Integer.valueOf(words[1]);
                //find the max word count
                if(word_freq>max_freq){
                    max_freq=word_freq;
                    max_word = words[0]+"\t"+max_freq;
                }
            }
            result.set(max_word);
            context.write(key,result);
        }
    }






    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MaxWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "maxword count");
        job2.setJarByClass(MaxWordCount.class);
        job2.setMapperClass(IdMapper.class);

        job2.setReducerClass(IntMaxReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
