package com.emtiaz.mmovies;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Created by imtiaz on 10/5/16.
 */
public class Movie implements Writable{
    private Integer movieId;
    private Integer year;
    private String title;
    private Float rating;

    public Integer getMovieId() {
        return movieId;
    }

    public Movie(){
    }

    public Movie(Movie m) {
        movieId = m.movieId;
        year = m.year;
        title = m.title;
        rating = m.rating;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }

    public String toSeparatedValues(String delimiter) {
        StringBuilder builder = new StringBuilder()
                .append(movieId).append(delimiter)
                .append(year).append(delimiter)
                .append(title).append(delimiter)
                .append(rating);
        return builder.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(movieId);
        dataOutput.writeInt(year);
        dataOutput.writeUTF(title);
        dataOutput.writeFloat(rating);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movieId = dataInput.readInt();
        year = dataInput.readInt();
        title = dataInput.readUTF();
        rating = dataInput.readFloat();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Movie movie = (Movie) o;
        return Objects.equals(movieId, movie.movieId) &&
                Objects.equals(year, movie.year) &&
                Objects.equals(title, movie.title) &&
                Objects.equals(rating, movie.rating);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId, year, title, rating);
    }
}
