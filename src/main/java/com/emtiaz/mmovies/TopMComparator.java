package com.emtiaz.mmovies;

import java.util.Comparator;

/**
 * Created by imtiaz on 10/6/16.
 */
public class TopMComparator implements Comparator<Movie> {
    @Override
    public int compare(Movie m1, Movie m2) {
        int val = m2.getRating().compareTo(m1.getRating());
        if(val == 0) {
            val = m2.getYear().compareTo(m1.getYear());
            if(val == 0) {
                val = m1.getTitle().compareTo(m2.getTitle());
            }
        }
        return val;
    }
}
