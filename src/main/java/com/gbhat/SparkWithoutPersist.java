package com.gbhat;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;

public class SparkWithoutPersist {
    public static void main(String[] args) throws IOException {
        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        Dataset<Row> cityDs = session.read().parquet("city.parquet");

        System.out.println("City:");
        cityDs.show();

        System.out.println("City in India:");
        cityDs.filter(cityDs.col("country_code").equalTo("IN")).show();

        System.out.println("City in USA:");
        cityDs.filter(cityDs.col("country_code").equalTo("US")).show();

        System.in.read();
    }
}
