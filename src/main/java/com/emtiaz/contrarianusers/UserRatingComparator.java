package com.emtiaz.contrarianusers;

import java.util.Comparator;

/**
 * Created by imtiaz on 10/6/16.
 */
public class UserRatingComparator implements Comparator<UserRating> {
    @Override
    public int compare(UserRating o1, UserRating o2) {
        int val = o2.getRating().compareTo(o1.getRating());
        if(val == 0) {
            o1.getUserId().compareTo(o2.getUserId());
        }
        return val;
    }
}
