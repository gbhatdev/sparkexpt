package com.gbhat;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;

public class SparkJoinWithPartition {
    public static void main(String[] args) throws IOException {
        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        session.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");       // DO NOT DO THIS. It is done for experiments here.

        Dataset<Row> cityDs = session.read().parquet("city.parquet");

        cityDs = cityDs.repartition(4, cityDs.col("country_code")).persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> countryDs = session.read().parquet("country.parquet");

        countryDs = countryDs.repartition(4, countryDs.col("iso2")).persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("Country:");
        countryDs.show();

        System.out.println("City:");
        cityDs.show();

        System.out.println("Joined:");
        Dataset<Row> joinedDs = cityDs.join(countryDs, countryDs.col("iso2").equalTo(cityDs.col("country_code")));
        joinedDs.show();

        System.in.read();

    }
}
