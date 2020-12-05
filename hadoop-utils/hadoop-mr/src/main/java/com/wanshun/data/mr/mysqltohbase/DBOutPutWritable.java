package com.wanshun.data.mr.mysqltohbase;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutPutWritable implements Writable, DBWritable {

    private String name;
    private int count;

    public DBOutPutWritable() {
    }

    public DBOutPutWritable(String name, int count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,name);
        preparedStatement.setInt(2,count);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        name = resultSet.getString(1);
        count =resultSet.getInt(2);

    }
}
