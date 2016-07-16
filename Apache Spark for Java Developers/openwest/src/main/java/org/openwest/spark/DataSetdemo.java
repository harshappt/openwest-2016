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
public class DataSetdemo {
    public static void main( String[ ] args ) {
        SparkConf conf = new SparkConf( ).setAppName( "RDDDemo" ).setMaster( "local[*]" );
        SparkContext sc = new SparkContext( conf );
        SparkSession ss = new SparkSession( sc );
        Dataset< Row > csvRowDS = ss.read( ).format( "csv" ).option( "header", true )
                .csv( "src/main/resources/us-500.csv" ).toDF( );
        csvRowDS.printSchema( );
        csvRowDS.createOrReplaceTempView( "people" );
        ss.sql( "select state, count(*) from people group by state " ).show( );
        csvRowDS.select( "state" ).groupBy( "state" ).count( ).show( );
        Dataset< Person > peopleDS = csvRowDS.withColumn( "zip", new Column( "zip" ).cast( DataTypes.IntegerType ) )
                .as( Encoders.bean( Person.class ) );
        peopleDS.show( );
    }
}
