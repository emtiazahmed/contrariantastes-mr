package com.emtiaz;

import com.emtiaz.mmovies.MMovies;
import com.emtiaz.mmovies.Movie;
import com.emtiaz.mmovies.TopMMovies;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

/**
 * Hello world!
 *
 */
public class App 
{
    /*public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: average-rating <in> <out>");
            System.exit(2);
        }

        /// 1st Job

        conf.set("mapreduce.map.memory.mb", "2048");

        Job job = Job.getInstance(conf, "Average Movie Rating");

        job.setJarByClass(MMovies.MMoviesMapper.class);

        job.setMapperClass(MMovies.MMoviesMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MMovies.MMoviesReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);



        job.addCacheFile(new URI("hdfs://localhost:9000/user/imtiaz/movies/movie_titles.txt"));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }*/

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: average-rating <in> <out>");
            System.exit(2);
        }

        /// 1st Job

        conf.set("mapreduce.map.memory.mb", "2048");

        Job job = Job.getInstance(conf, "Average Movie Rating");

        job.setJarByClass(TopMMovies.TopMMapper.class);

        job.setMapperClass(TopMMovies.TopMMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Movie.class);

        job.setReducerClass(TopMMovies.TopMReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI("hdfs://localhost:9000/user/imtiaz/movies/movie_titles.txt"));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
