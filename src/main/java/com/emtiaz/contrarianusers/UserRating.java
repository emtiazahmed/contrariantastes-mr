package com.emtiaz.contrarianusers;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by imtiaz on 10/6/16.
 */
public class UserRating implements Writable {
    private Integer userId;
    private Float rating;

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }


    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(userId);
        dataOutput.writeFloat(rating);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId = dataInput.readInt();
        rating = dataInput.readFloat();
    }
}
