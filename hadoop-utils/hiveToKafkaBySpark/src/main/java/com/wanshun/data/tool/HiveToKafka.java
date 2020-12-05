package com.wanshun.data.tool;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

public class HiveToKafka implements Serializable{

    public static void main(String[] args) {

//        if (args.length < 4) {
//            throw new RuntimeException("table|sql|bootstrapServers|topic not allowed to be empty.");
//        }
//
//        final String tableName = args[0];
//        final String sql = args[1];
//        final String bootstrapServers = args[2];
//        final String topicName = args[3];

        final String tableName = "testTable";
        final String sql = "select * from default.test";
        final String bootstrapServers = "172.18.5.156:9092";
        final String topicName = "dataMpl-usercenter";

        System.out.println("tableName = " + tableName);
        System.out.println("sql = " + sql);
        System.out.println("bootstrapServers = " + bootstrapServers);
        System.out.println("topicName = " + topicName);

        //1、准备配置文件 参考:ProducerConfig.java
        final Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("connections.max.idle.ms", 1800000L);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        long begin = System.currentTimeMillis();
        System.out.println("this is HiveToKafka  start---------------");

        /**定义spark，定义hive内部表所在目录*/
//        SparkSession spark = getSparkSession();
        SparkSession spark = getSparkSessionTest();

        Dataset<Row> datasetRow = spark.sql(sql);
        spark.sql("show databases").show();
        spark.sql("show tables").show();
        datasetRow.show();
        datasetRow.foreachPartition(new ForeachPartitionFunction<Row>() {
            public void call(Iterator<Row> iterator) throws Exception {
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    System.out.println(row.get(0)+"\n"+row.get(1)+"\n"+row.get(2));

                    StructType schema = row.schema();
                    StructField[] fields = schema.fields();
                    for (int i = 0; i < fields.length; i++) {

                        String name = fields[i].name();
                        String value = row.get(i).toString();
                        System.out.println(name+":"+value);


                    }

                }
            }
        });

        /*
        try{
            Dataset<Row> dataset = spark.sql(sql);
            dataset.foreachPartition(new ForeachPartitionFunction<Row>() {
                @Override
                public void call(Iterator<Row> iterator) {

                    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

                    while(iterator.hasNext()){

                        try{
                            //遍历一张表里的每一条数据
                            Row row = iterator.next();
//                            String balanceId  = row.get(0).toString();
                            String driverId  = row.get(1).toString();
                            kafkaProducer.send(new ProducerRecord<String, String>(topicName, toJson(row, driverId)));
                        }catch (Exception e){
                            e.printStackTrace();
                        }

                    }

                    kafkaProducer.flush();
                    kafkaProducer.close();
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }
        */

        System.out.println("hiveToKafka use time : " + (System.currentTimeMillis() - begin));
    }

    /**
     *
     * @param row
     * @param driverId
     * @return
     */
    private static String toJson(final Row row, final String driverId) {
        JsonObject kafkaEvent = new JsonObject();
        JsonObject record = new JsonObject();

        kafkaEvent.addProperty("clusterEventType", 100);
        kafkaEvent.addProperty("balanceId", driverId);


        StructType schema = row.schema();

        StructField[] fields = schema.fields();

        for(int i=0; i<fields.length; i++){

            try {
                String typeName = fields[i].dataType().typeName();

                if ("integer".equals(typeName) || "int".equals(typeName) || "bigint".equals(typeName) || "long".equals(typeName)) {
                    Number fieldValue = (Number) row.getAs(fields[i].name());
                    record.addProperty(fields[i].name(), fieldValue);
                } else {
                    String fieldValue = String.valueOf(row.getAs(fields[i].name()));
                    if ("null".equals(fieldValue)) {
                        fieldValue = "";
                    }
                    record.addProperty(fields[i].name(), fieldValue);


                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        kafkaEvent.addProperty("data", record.toString());
        return kafkaEvent.toString();
    }

    /**
     *
     * @return
     */
    public static SparkSession getSparkSession(){

        /**定义spark，定义hive内部表所在目录*/
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive To Kafka")
                .master("local[*]")
//                .config("spark.sql.warehouse.dir","E:/IDEA Project/workspace/BigDateProject/hadoop-utils/spark-sql/spark-warehouse")
                .config("hadoop.home.dir","/user/hive/spark-warehouse")
                .enableHiveSupport()
                .getOrCreate();
        return spark;
    }

    public static SparkSession getSparkSessionTest(){

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
