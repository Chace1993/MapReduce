import java.io.File;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bow {

    private final static String[] top100Word = { "the", "be", "to", "of", "and", "a", "in", "that", "have", "i", "it",
            "for", "not", "on", "with", "he", "as", "you", "do", "at", "this", "but", "his", "by", "from", "they", "we",
            "say", "her", "she", "or", "an", "will", "my", "one", "all", "would", "there", "their", "what", "so", "up",
            "out", "if", "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time", "no", "just",
            "him", "know", "take", "people", "into", "year", "your", "good", "some", "could", "them", "see", "other", "than",
            "then", "now", "look", "only", "come", "its", "over", "think", "also", "back", "after", "use", "two", "how", "our",
            "work", "first", "well", "way", "even", "new", "want", "because", "any", "these", "give", "day", "most", "us" };

    public static List<String> Top100WordArray = null;


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text k = new Text();
        private Text v = new Text();
        private String pattern = "[^a-zA-Z0-9]";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            String line = value.toString();//
            line = line.replaceAll(pattern, " ").toLowerCase();//
            StringTokenizer itr = new StringTokenizer(line);//
            while (itr.hasMoreTokens()) {
                k.set(fileName);
                v.set(itr.nextToken());
                context.write(k, v);
            }
        }
    }



    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Vector<Integer>> {

        private Vector <Integer> numVector=null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Top100WordArray=Arrays.asList(top100Word);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Text,Vector<Integer>> hashmap =new HashMap<Text, Vector<Integer>>();
            for(Text value:values){
                if(Top100WordArray.contains(value.toString())){
                    Vector <Integer> numVector =hashmap.get(key);
                    if(numVector==null){
                        numVector= new Vector<Integer>();
                        for(int i=0;i<Top100WordArray.size();i++){
                            numVector.add(0);
                        }
                    }
                    int pos =Top100WordArray.indexOf(value.toString());
                    numVector.set(pos,numVector.get(pos)+1);
                    hashmap.put(key,numVector);
                }
            }

            for(Map.Entry<Text,Vector<Integer>> map : hashmap.entrySet()){
                context.write(map.getKey(),map.getValue());
            }

        }

//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            super.cleanup(context);
//
//        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "word count");
        job.setJarByClass(Bow.class);
        job.setMapperClass(TokenizerMapper.class);
       // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Vector.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        File file =new File("./output");
        if  (file .exists()  && file .isDirectory())
        {
            System.out.println("delete file");
            deleteDir(file);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}
