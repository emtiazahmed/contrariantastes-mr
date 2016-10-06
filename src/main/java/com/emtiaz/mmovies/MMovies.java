package com.emtiaz.mmovies;

import com.emtiaz.ContrarianConstants;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * Created by imtiaz on 10/4/16.
 */
public class MMovies {

    public final static Logger log = Logger.getLogger(MMovies.class.getName());

    public final static String DELIMITER = ",";
    public final static String DELIMITER_TAB = "\t";

    public static class MMoviesMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

        IntWritable movieId = new IntWritable();
        IntWritable rating = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), DELIMITER);
            movieId.set(Integer.parseInt(tokenizer.nextToken()));
            tokenizer.nextToken();// ignore userId
            rating.set(Integer.parseInt(tokenizer.nextToken()));
            context.write(movieId, rating);
        }
    }

    public static class MMoviesReducer extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {

        FloatWritable average = new FloatWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            long sum = 0;
            for (IntWritable rating:values) {
                count++;
                sum += rating.get();
            }

            if(count >= ContrarianConstants.R) {
                average.set(sum/count);
                context.write(key, average);
            }
        }
    }



}
