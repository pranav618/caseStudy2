package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;


public class IotSystem {

    public Dataset<Row> returnReport(String inputIot){
        Dataset<Row> iotResult= readCsvFile(inputIot);
        Dataset<Row> maxTemperature= readCsvFile("src/main/resources/dataset/maxTemperature.csv");
        Dataset<Row> iotDataFrame= null;
        Dataset<Row> iotDataFrame_average= null;

        iotDataFrame=iotResult.join(maxTemperature,iotResult.col("device_id").equalTo(maxTemperature.col("device_id")),"inner")
        .select(iotResult.col("*"), maxTemperature.col("max_temp"));

        iotDataFrame_average = iotDataFrame.groupBy("device_id").agg(mean(col("temperature")).alias("average_temp"));
        iotDataFrame = iotDataFrame.join(iotDataFrame_average
                ,iotDataFrame.col("device_id").equalTo(iotDataFrame_average.col("device_id"))
                ,"inner"
        ).select(iotDataFrame.col("*")
                ,iotDataFrame_average.col("average_temp")
        );

        iotDataFrame= iotDataFrame
                .withColumn("temperature", col("temperature").cast(DataTypes.DoubleType))
                .withColumn("max_temp", col("max_temp").cast(DataTypes.DoubleType));

        Dataset<Row> tempGTMaxTemp = iotDataFrame.where(col("temperature").gt(col("max_temp")));

        Dataset<Row> tempGTAverageTemp = iotDataFrame.where(col("temperature").gt(col("average_temp")));

        tempGTMaxTemp.show();
        System.out.println("When Temperature is greater than Max Temperature");

        tempGTAverageTemp.show();
        System.out.println("When Temperature is greater than Average Temperature");

        return iotDataFrame;

    }

    public SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("global-sales")
                .master("local[*]")
                .config("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")
                .getOrCreate();
    }

    public Dataset<Row> readCsvFile(String inputDirectory){
        SparkSession sparkSession = getSparkSession();
        return sparkSession.read().option("delimeter",":").option("header","true").csv(inputDirectory);

    }

}
