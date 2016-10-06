package com.emtiaz.mmovies;

import com.emtiaz.ContrarianConstants;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by imtiaz on 10/4/16.
 */
public class TopMMovies {

    public final static Logger log = Logger.getLogger(TopMMovies.class.getName());

    public static class TopMMapper extends Mapper<Object, Text, NullWritable, Movie> {
        public final static String DELIMITER = ",";
        private static HashMap<Integer, Movie> movies = new HashMap<Integer, Movie>();
        private static SortedSet<Movie> topMMovies = new TreeSet<Movie>(new TopMComparator());

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            log.info("value="+value.toString());
            String[] values = value.toString().split("\t");
            log.info(values[0] + "," + values[1]);
            Integer movieId = Integer.valueOf(values[0]);
            Float rating = Float.valueOf(values[1]);
            Movie movie = movies.get(movieId);
            movie.setRating(rating);
            topMMovies.add(movie);
            if(topMMovies.size() > ContrarianConstants.M) {
                topMMovies.remove(topMMovies.last());
            }
        }


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try (BufferedReader br = new BufferedReader(new FileReader("movie_titles.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    int start = 0, end = 0;
                    Movie movie = new Movie();
                    end = line.indexOf(DELIMITER);
                    movie.setMovieId(Integer.valueOf(line.substring(start, end)));
                    start = end + 1;
                    end = line.indexOf(DELIMITER, start);
                    String year = line.substring(start, end);
                    if(NumberUtils.isNumber(year))
                        movie.setYear(Integer.valueOf(year));
                    else
                        movie.setYear(0);
                    start = end + 1;
                    movie.setTitle(line.substring(start));
                    movies.put(movie.getMovieId(), movie);
                }

                log.info(movies.size() + " movies loaded.....");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Movie movie:topMMovies) {
                context.write(NullWritable.get(), movie);
            }
        }
    }

    public static class TopMReducer extends Reducer<NullWritable, Movie, NullWritable, Text> {
        private Text outputText = new Text();
        private TreeSet<Movie> topMMovies = new TreeSet<Movie>(new TopMComparator());

        @Override
        protected void reduce(NullWritable key, Iterable<Movie> values, Context context) throws IOException, InterruptedException {
            int count  = 0;
            for (Movie movie: values) {
                Movie tempMovie = new Movie(movie);
                log.info("movie exists in topMMovies? " + topMMovies.contains(tempMovie));
                count++;
                log.info(movie.toSeparatedValues("\t"));
                log.info(String.valueOf(topMMovies.add(tempMovie)));
                if(topMMovies.size() > ContrarianConstants.M) {
                    log.info("removing movie");
                    topMMovies.pollLast();
                }
            }
            log.info("reducer received " + count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Movie movie: topMMovies) {
                outputText.set(movie.toSeparatedValues("\t"));
                context.write(NullWritable.get(), outputText);
            }
        }
    }

}
