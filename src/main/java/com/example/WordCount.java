package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    BufferedReader reader = new BufferedReader(new FileReader(Paths.get(cacheFile).toString()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                    reader.close();
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 1) {
                String headline = fields[1].toLowerCase().replaceAll("[^a-zA-Z ]", " ");
                StringTokenizer itr = new StringTokenizer(headline);
                while (itr.hasMoreTokens()) {
                    String token = itr.nextToken().trim();
                    if (!stopWords.contains(token)) {
                        word.set(token);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: WordCount <input path> <output path> <stop words path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new URI(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}