package com.emtiaz.contrarianusers;


import com.emtiaz.ContrarianConstants;
import com.emtiaz.mmovies.Movie;
import com.emtiaz.mmovies.TopMMovies;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * Created by imtiaz on 10/6/16.
 */
public class ContrarianUsers {

    public final static String DELIMITER = ",";

    public static class ContrarianUsersMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable userId = new IntWritable();
        private IntWritable rating = new IntWritable();
        private static HashSet<Integer> movies = new HashSet<Integer>();
        public final static Logger log = Logger.getLogger(ContrarianUsersMapper.class.getName());

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try (BufferedReader br = new BufferedReader(new FileReader("part-r-00000"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    movies.add(Integer.valueOf(parts[0]));
                }

                log.info(movies.size() + " movies loaded.....");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString(), DELIMITER);

            Integer movieId = Integer.parseInt(tokenizer.nextToken());
            userId.set(Integer.parseInt(tokenizer.nextToken()));// ignore userId
            rating.set(Integer.parseInt(tokenizer.nextToken()));

            if(movies.contains(movieId)) {
                context.write(userId, rating);
            }
        }
    }

    public static class ContrarianUsersReducer extends Reducer<IntWritable, IntWritable, NullWritable, Text> {
        FloatWritable average = new FloatWritable();
        Text outputText = new Text();
        @Override
        protected void reduce(IntWritable userId, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            long sum = 0;
            for (IntWritable rating:values) {
                count++;
                sum += rating.get();
            }
            average.set(sum/count);
            outputText.set(userId.get() + "\t" + average.get());
            context.write(NullWritable.get(), outputText);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: average-rating <in> <out>");
            System.exit(2);
        }

        /// 1st Job

        conf.set("mapreduce.map.memory.mb", "2048");

        Job job = Job.getInstance(conf, "Average User Rating");

        job.setJarByClass(ContrarianUsers.ContrarianUsersMapper.class);

        job.setMapperClass(ContrarianUsers.ContrarianUsersMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ContrarianUsers.ContrarianUsersReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI("hdfs://localhost:9000/user/imtiaz/topnmovies/part-r-00000"));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
