import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bitnami on 9/24/16.
 */
public class NGramFreq {
    public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {
        int noGram;
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            noGram = Integer.valueOf(conf.get("noGram"));
        }

        //map method
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String words[];
            words = value.toString().split("\\s+");

            if (words.length < 2) {
                return;
            }

            for (int i = 0; i < words.length ; ++i ) {
                StringBuffer sb = new StringBuffer();
                if(i<(words.length-(noGram-1))){
                  for(int j=0;j<noGram;j++){
                    sb.append(words[i+j]);
                    if(j<noGram-1){
                        sb.append(" & ");
                    }
                  }
                    context.write(new Text(sb.toString().trim()), one);
               }

            }

        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();



        conf.set("noGram", args[2]);


        Job job = new Job(conf, "Count NGrams");

        job.setJarByClass(NGramFreq.class);
        job.setMapperClass(NGramFreq.NGramMapper.class);
        job.setCombinerClass(NGramFreq.NGramReducer.class);
        job.setReducerClass(NGramFreq.NGramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

