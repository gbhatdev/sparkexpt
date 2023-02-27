package com.gbhat;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;

public class SparkQueryVsAPI {
    public static void main(String[] args) throws IOException {
        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        Dataset<Row> cityDs = session.read().parquet("city.parquet").persist(StorageLevel.MEMORY_AND_DISK());
        Dataset<Row> countryDs = session.read().parquet("country.parquet").persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("City:");
        cityDs.show();

        System.out.println("Country:");
        countryDs.show();

        cityDs.createOrReplaceTempView("City");
        countryDs.createOrReplaceTempView("Country");

        System.in.read();

        System.out.println("Filter API:");
        cityDs.filter(cityDs.col("country_code").equalTo("IN")).show();

        System.out.println("Filter Query:");
        session.sql("select * from City where `country_code` == 'IN'").show();

        System.in.read();

        System.out.println("Aggregate API:");
        cityDs.join(countryDs, cityDs.col("country_code").equalTo(countryDs.col("iso2")))
                .filter(cityDs.col("country_code").equalTo("IN").and(cityDs.col("state_code").equalTo("KA"))).show();

        System.out.println("Aggregate Query:");
        session.sql("select * from City inner join Country on City.country_code == Country.iso2 where City.country_code == 'IN' and City.state_code='KA'").show();
    }
}
