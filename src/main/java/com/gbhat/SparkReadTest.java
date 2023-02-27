package com.gbhat;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkReadTest {
    public static void main(String[] args) throws IOException {
        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
//        readConvertCSV(session);

        Dataset<Row> ds = session.read().parquet("city.parquet");


        ds = ds.select("name", "country_code");

//        ds = ds.filter(ds.col("country_code").equalTo("IN"));
        ds.show(100, false);

        System.in.read();
    }

    private static void readConvertCSV(SparkSession session) {
        Dataset<Row> ds = session.read().option("header", "true").option("inferSchema", "true").csv("city.csv");
        ds.printSchema();
        ds.show();
        ds.repartition(1).write().parquet("city.parquet");

        session.read().option("header", "true").option("inferSchema", "true").csv("country.csv").repartition(1).write().parquet("country.parquet");
    }
}
