package com.bdec.training.spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

public class IotSystemTest {

    IotSystem iotSystem = new IotSystem();

    public SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("global-sales")
                .master("local[*]")
                .config("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")
                .getOrCreate();
    }

    @Test
    public void returnReport() {
        Dataset<Row> testData= null;
        testData= iotSystem.returnReport("src/main/resources/dataset/iotDataset.csv");
        Assert.assertTrue(testData.count()>0);
    }
}
