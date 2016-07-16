package org.openwest.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/*
 * The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection of
 * elements partitioned across the nodes of the cluster that can be operated on in parallel.
 */
public class RDDOpenWestDemo {
    public static void main( String[ ] args ) {
        SparkConf conf = new SparkConf( ).setAppName( "RDDDemo" ).setMaster( "local[*]" );
        JavaSparkContext sc = new JavaSparkContext( conf );
        List< Integer > list = Arrays.asList( 1, 2, 3, 4, 5, 6, 67, 7, 8, 99, 23948, 23 );
        JavaRDD< Integer > javaRDD = sc.parallelize( list );
        javaRDD.filter( i -> i % 2 == 0 ).collect( );
        /*
         * Spark can create distributed datasets from any storage source supported by Hadoop, including local file
         * system, HDFS, Cassandra, HBase, Amazon S3, etc. Spark supports text files, SequenceFiles, and any other
         * Hadoop InputFormat.
         */
        JavaRDD< String > inputRDD = sc.textFile( "src/main/resources/us-500.csv", 3 );
        JavaRDD< String[ ] > stringsArray = inputRDD.map( line -> line.split( "," ) );
        JavaPairRDD< String, Integer > pairRDD = stringsArray.mapToPair( strings -> new Tuple2<>( strings[ 6 ], 1 ) );
        JavaPairRDD< String, Integer > resultRDD = pairRDD.reduceByKey( ( a, b ) -> a + b ).sortByKey( );
        resultRDD.collect( ).forEach( result -> System.out.println( result._1 + "  " + result._2 ) );
    }
}
