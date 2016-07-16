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
public class RDDDemo {
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
        JavaRDD< String[ ] > stringArray = inputRDD.map( line -> line.split( "," ) );
        JavaPairRDD< String, Integer > tuplesRDD = stringArray
                .mapToPair( strArray -> new Tuple2< String, Integer >( strArray[ 6 ], 1 ) );
        JavaPairRDD< String, Integer > reduceByKeyRDD = tuplesRDD.reduceByKey( ( a, b ) -> a + b ).sortByKey( );
        reduceByKeyRDD.collect( ).forEach( t -> System.out.println( t._1 + " " + t._2 ) );
    }

    /**
     * RDD by parallelizing the existing collection
     * 
     * @param sc
     */
    private static void rddFromCollection( JavaSparkContext sc ) {
        List< Integer > intList = Arrays.asList( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 );
        JavaRDD< Integer > intRDD = sc.parallelize( intList );
        JavaRDD< Integer > evenIntRDD = intRDD.filter( i -> i % 2 == 0 );
        Integer sum = evenIntRDD.reduce( ( Integer a, Integer b ) -> a + b );
        System.out.println( sum );
    }
}