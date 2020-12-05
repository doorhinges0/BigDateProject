package com.wanshun.data.mr.mysqltohbase;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBInputWritable implements Writable, DBWritable {
//    private String etl_day;
//    private Long driverid;
//    private String dateday;
//    private Integer thelast7daymaxordercnt;
//    private String thelast7daymaxorderdate;
//    private Integer thelast30daymaxordercnt;
//    private String thelast30daymaxorderdate;
//    private Integer thelastweekmaxordercnt;
//    private String thelastweekmaxorderdate;
//    private Integer thelastmonthmaxordercnt;
//    private String thelastmonthmaxorderdate;
//    private Integer ordercnt;
//    private String agencynumber;

    private int id;
    private String name;


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1,id);
        preparedStatement.setString(2,name);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        name = resultSet.getString(2);

    }
}
