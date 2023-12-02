package com.moncif;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.functions.*;
/**
 * Hello world!
 *
 */
public class AppMain
{
    public static void main( String[] args ) throws IOException
    {
        // 0. charger la configuration
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");
        Properties props = new Properties();
        props.load(is);
        // 1. creer un SparkConf
        SparkConf conf = new SparkConf();
        conf.setAppName("ex2-spark");
        conf.setMaster("local[*]");
        // 2. creer un SparkSession
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        session.sparkContext().log().warn("ex2-spark");
        // 3. Lire un fichier CSV
        Dataset<Row> licencesDS = session.read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", ";")
                .option("encoding", "UTF-8")
                .option("inferSchema", "true")
                .load("lic-data-2019.csv");
        licencesDS.cache();
        // 4. construire une table en session
        licencesDS.createOrReplaceTempView("licences");
        String request = props.getProperty("footrequest");
        Dataset<Row> result = session.sql(request);
        result.show();
        // 5. idem au 4. mais avec l'API Dataset
        Dataset<Row> footStats = licencesDS.select(substring(col("code_commune"), 1, 2).as("dep"),
                        col("fed_2019"),
                        col("l_2019"),
                        col("pop_2018"))
                .where(col("fed_2019").equalTo("111"))
                .groupBy(col("dep"), col("fed_2019"))
                .sum("l_2019", "pop_2018")
                .withColumnRenamed("sum(l_2019)", "total_lic")
                .withColumnRenamed("sum(pop_2018)", "total_pop")
                .withColumn("total", col("total_lic").multiply(100.0).divide(col("total_pop")))
                .orderBy(desc("total"))
                .limit(20);
        footStats.show(false);
        // 6. externaliser dans une table Postgresql
        footStats.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/sparkyy")
                .option("driver", "org.postgresql.Driver")
                .option("user", "postgres")
                .option("password", "uuuu")
                .option("dbtable", "foot_stats")
                .mode(SaveMode.Overwrite)
                .save();

        session.stop();
    }
}
