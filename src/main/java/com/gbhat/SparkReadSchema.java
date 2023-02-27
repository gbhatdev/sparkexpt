package com.gbhat;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkReadSchema {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        /*
               Read CSV Infer Schema
         */
        Dataset<Row> cityDs = session.read()
                .option("header", "true")
                .option("inferSchema", "true")      //Use it only for experiments
                .csv("city.csv");

        System.out.println("City with infer schema:");
        cityDs.show();
        cityDs.printSchema();


        /*
            Read CSV with Schema
         */

        StructType schema = new StructType()
                .add("Id", DataTypes.IntegerType)
                .add("Name", DataTypes.StringType)
                .add("State Id", DataTypes.IntegerType)
                .add("State Code", DataTypes.StringType)
                .add("State Name", DataTypes.StringType)
                .add("Country Id", DataTypes.IntegerType)
                .add("Country Code", DataTypes.StringType)
                .add("Country Name", DataTypes.StringType)
                .add("Latitude", DataTypes.DoubleType)
                .add("Longitude", DataTypes.DoubleType)
                .add("Wiki Data Id", DataTypes.StringType);

        System.out.println("City with explicit schema:");
        Dataset<Row> cityDs1 = session.read()
                .option("header", "true")
                .schema(schema)
                .csv("city.csv");

        cityDs1.show();
        cityDs1.printSchema();
    }
}
