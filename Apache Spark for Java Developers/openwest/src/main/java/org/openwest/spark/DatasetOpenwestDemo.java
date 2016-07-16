package org.openwest.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/*
 * A [[Dataset]] is a strongly typed collection of domain-specific objects that can be transformed
 * in parallel using functional or relational operations. Each Dataset also has an untyped view
 * called a [[DataFrame]], which is a Dataset of [[Row]].
 *
 */
public class DatasetOpenwestDemo {
    public static void main( String[ ] args ) {
        SparkConf conf = new SparkConf( ).setAppName( "RDDDemo" ).setMaster( "local[*]" );
        SparkContext sc = new SparkContext( conf );
        SparkSession ss = new SparkSession( sc );
        Dataset< Row > untypedDS = ss.read( ).format( "csv" ).option( "header", true )
                .csv( "src/main/resources/us-500.csv" ).toDF( );
        untypedDS.printSchema( );
        untypedDS.createOrReplaceTempView( "people" );
        Dataset< Row > sqlResult = ss.sql( "select state, count(*) from people  group by state order by state" );
        sqlResult.show( );
        sqlResult.where( "state = 'CA'" ).show( );
        Dataset< Person > personDataSet = untypedDS
                .withColumn( "zip", new Column( "zip" ).cast( DataTypes.IntegerType ) )
                .as( Encoders.bean( Person.class ) );
        personDataSet.filter( p -> p.getState( ).equals( "CA" ) ).collectAsList( )
                .forEach( p -> System.out.println( p.getState( ) + " " + p.getZip( ) ) );
        // TODO RDD definition
        // TODO SparkConf and SparkContext
        // TODO WebUI
        // TODO RDD from List
        // TODO RDD from Transformations
        // TODO RDD from Textfile
        // TODO Lazy execution
        // TODO define Dataset and Dataframe
        // TODO spark conf and spark session
        // TODO 2 ways of creating DataSet
        // TODO DataSet<ROW> from file
        // TODO SQL using DataSet
        // TODO DSL example - where state is CA
        // TODO Data set of strongly typed domain object
        // TODO RDD like operation on concrete Dataset
        // TODO Benefits of using Datasets over RDD
    }
}
