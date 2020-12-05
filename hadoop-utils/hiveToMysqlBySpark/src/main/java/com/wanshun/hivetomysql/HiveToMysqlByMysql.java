package com.wanshun.hivetomysql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;
/*
  -- 10分钟 1800w 并行度太高，机器cpu占满
   spark-submit --class com.wanshun.data.tool.HiveToMysql --master yarn  --deploy-mode cluster --driver-memory 1g --executor-memory  5g  --executor-cores 10  --num-executors 10 \
  /home/pubuser/xxu/order_reward/HiveToMysql.jar \
   "jdbc:mysql://172.21.78.12:3306/orderreward?characterEncoding=utf8&relaxAutoCommit=true&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&autoReconnect=true&socketTimeout=30000&connectTimeout=30000&rewriteBatchedStatements=true"
  "orderreward" "YWfD_nz6jlBJCtQ2tN2q6yZqt6mxYQ" "dwd_driver_order_inc_record_day"  \
  "select driverId,dateDay,orderId,agencyNumber,updateTime,createTime from default.dwd_driver_reward_order_inc_record_day"

  -- 43秒 141W  配置比较正常
     spark-submit --class com.wanshun.data.tool.HiveToMysql --master yarn  --deploy-mode cluster --driver-memory 1g --executor-memory  5g  --executor-cores 10  --num-executors 10 \
  /home/pubuser/xxu/order_reward/HiveToMysql.jar \
   "jdbc:mysql://172.21.78.12:3306/orderreward?characterEncoding=utf8&relaxAutoCommit=true&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&autoReconnect=true&socketTimeout=30000&connectTimeout=30000&rewriteBatchedStatements=true"
  "orderreward" "YWfD_nz6jlBJCtQ2tN2q6yZqt6mxYQ" "dwd_driver_order_max_cnt_day"  \
   "select driverid ,dateday ,NVL(thelast7daymaxordercnt,0) thelast7daymaxordercnt,thelast7daymaxorderdate,NVL(thelast30daymaxordercnt,0) thelast30daymaxordercnt,thelast30daymaxorderdate,NVL(thelastweekmaxordercnt,0) thelastweekmaxordercnt,thelastweekmaxorderdate,NVL(thelastmonthmaxordercnt,0) thelastmonthmaxordercnt,thelastmonthmaxorderdate,NVL(ordercnt,0) ordercnt,agencynumber,updatetime,createtime from dws_driver_order_max_cnt_day where dateday_p=dateday_p"

 */
public class HiveToMysqlByMysql {
    public static void main(String[] args) {

//        if (args.length < 4) {
//            throw new RuntimeException("table|sql|bootstrapServers|topic not allowed to be empty.");
//        }
//
//        final String tableName = args[0];
//        final String sql = args[1];
//        final String bootstrapServers = args[2];
//        final String topicName = args[3];

//        final String sql = "select  driverId,dateDay,orderId,agencyNumber,updateTime,createTime from default.dwd_driver_reward_order_inc_record_day";
        final String sql = "select dt,name,NVL(age,0) age,NVL(sex,'') sex from default.hive_test where dt='2020-11-01'";
        long begin = System.currentTimeMillis();
        System.out.println("this is sync  start---------------");

        /**定义spark，定义hive内部表所在目录*/
//        SparkSession spark = getSparkSession();
        SparkSession spark = getSparkSessionTest();

        Dataset<Row> datasetRow = spark.sql(sql);
        datasetRow.show();

        //准备配置文件
        final Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "666666");
        String url = "jdbc:mysql://192.168.10.15:3306/orderreward?characterEncoding=utf-8";
        String targetTable = "hive_test";

//    datasetRow.write().mode(SaveMode.Overwrite).jdbc(url,table,props);
        //根据字段插入
        datasetRow.write().mode(SaveMode.Append).jdbc(url, targetTable, props);

        spark.stop();

    }

    /**
     * @return
     */
    public static SparkSession getSparkSession() {

        /**定义spark，定义hive内部表所在目录*/
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive To Kafka")
                .master("local[*]")
//                .config("spark.sql.warehouse.dir","E:/IDEA Project/workspace/BigDateProject/hadoop-utils/spark-sql/spark-warehouse")
                .config("hadoop.home.dir", "/user/hive/spark-warehouse")
                .enableHiveSupport()
                .getOrCreate();
        return spark;
    }

    public static SparkSession getSparkSessionTest() {

        /**定义spark，定义hive内部表所在目录*/
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("hive")
                .enableHiveSupport()
                .getOrCreate();

        return spark;
    }


}
