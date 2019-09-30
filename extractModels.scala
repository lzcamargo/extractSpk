// Extract models from input models in XMI or JSON format to DataFrame
// Next, the Dataframe with model is translated to GraphFrame
// There are Motif, inDgrees and outDgrees codes to
// This code was used as base for the algorithms and transformation executions in spark-shell on local mode. 

==================== Extractor and Translator of Models to GraphFrame===================================

import org.apache.spark._
import java.io._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType, DataType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import scala.collection.immutable.IntMap
import scala.collection.mutable.ArrayLike
import scala.collection.mutable.WrappedArray
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.{min, max}

spark.conf.set("spark.sql.crossJoin.enabled", true)

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val modelpath = "hdfs://127.0.0.1:9868/models/Families.xmi"
val modelDF = if (modelpath.endsWith(".json")) {spark.read.option("multiline", "true").json(modelpath)} else {sqlContext.read.format("com.databricks.spark.xml").option("rootTag", "xmi:XMI").option("rowTag", "xmi:XMI").load(modelpath)}

val TT_NONE   = 0;
val TT_ROW    = 1;
val TT_STRUCT = 2;
val TT_ARRAY  = 3;
val TT_LEAF   = 4;
val TT_NULL   = 5;

type NodeType = (Int, Int, String, String);
type NodeList = List[NodeType];
type EdgeType = (Int, Int, String);
type EdgeList = List[EdgeType];
type verticesAndEdges = (NodeList, EdgeList)

val nodeMaxIdBy: (NodeType) => Int = (n) => n._1
val structFieldGetKey: (StructField) => String = (sf) => sf.name

def model2graph_schema( modelItem:Object, schemaItem:Object, startId:Int, parsingRow:Boolean = false ): verticesAndEdges =
{
    var id = startId;
    var vertices = List[NodeType]();
    var edges = List[EdgeType]();

    if(modelItem == null)
    {
        // we have a null value
        vertices = vertices :+ ( id, TT_NULL, "", "__NULL__" );
    }
    else if(!parsingRow)
    {
        // we have in our hands an Array of Rows
        // in this case the schema does not reflect this array,
        // it merely describes a row in itself!
        // That's why we just pass the entire schemaItem forward
        vertices = vertices :+ (( id, TT_ARRAY, "", "ArrayType" ));    // the node representing this array of rows
        var rowArr = modelItem.asInstanceOf[Array[Row]]
        var subId:Int = startId + 1;
        var idx:Int = 0;
        for( rw <- rowArr )
        {
            // for each row in the model
            var res:verticesAndEdges = (null, null);
            res = model2graph_schema( rw, schemaItem, subId, true );
            vertices = vertices ++ res._1;
            edges = edges ++ res._2;

            // generate edge
            edges = edges :+ (( id, subId, idx.toString ));

            // find out maximum node id assigned during recursion and update next id
            subId = ( res._1.maxBy( nodeMaxIdBy )._1 ) + 1;

            idx = idx + 1;
        }
    }
    else
    {
        // we have already passed the array of rows, parse using the schema
        if(schemaItem.isInstanceOf[StructType])
        {
            var arrVar:Seq[_] = null;
            if(modelItem.isInstanceOf[Row])
                arrVar = modelItem.asInstanceOf[Row].toSeq;
            else
                arrVar = modelItem.asInstanceOf[WrappedArray[_]];

            var arrIdx:Int = 0;
            var keys:Seq[String] = null;
            keys = schemaItem.asInstanceOf[StructType].map( structFieldGetKey );
            vertices = vertices :+ (( id, TT_STRUCT, "", "StructType" ));    // the node representing this struct
            var subId:Int = startId + 1;
            for(key <- keys)
            {
                // for each element of the struct
                var res:verticesAndEdges = (null, null);
                res = model2graph_schema( arrVar(arrIdx).asInstanceOf[Object], schemaItem.asInstanceOf[StructType].apply(key).dataType, subId, true );
                vertices = vertices ++ res._1;
                edges = edges ++ res._2;

                // generate edge
                edges = edges :+ (( id, subId, key ));

                // find out maximum node id assigned during recursion and update next id
                subId = ( res._1.maxBy( nodeMaxIdBy )._1 ) + 1;

                arrIdx = arrIdx + 1;
            }
        }
        else if(schemaItem.isInstanceOf[ArrayType])
        {
            var arrVar = modelItem.asInstanceOf[WrappedArray[_]];
            var arrIdx:Int = 0;
            vertices = vertices :+ (( id, TT_ARRAY, "", "ArrayType" ));    // the node representing this array
            var subId:Int = startId + 1;
            for(arrItem <- arrVar)
            {
                // for each element of the array
                var res:verticesAndEdges = (null, null);
                res = model2graph_schema( arrVar(arrIdx).asInstanceOf[Object], schemaItem.asInstanceOf[ArrayType].elementType, subId, true );
                vertices = vertices ++ res._1;
                edges = edges ++ res._2;

                // generate edge
                edges = edges :+ (( id, subId, arrIdx.toString ));

                // find out maximum node id assigned during recursion and update next id
                subId = ( res._1.maxBy( nodeMaxIdBy )._1 ) + 1;

                arrIdx = arrIdx + 1;
            }
        }
        else if(schemaItem.isInstanceOf[StructField])
        {
            System.err.println("Warning: StructField being processed as the schemaItem!");
            vertices = vertices :+ (( id, TT_LEAF, modelItem.toString, "" ));
        }
        else if(schemaItem.isInstanceOf[DataType])
        {
            // a leaf data type, let's just make a leaf
            vertices = vertices :+ (( id, TT_LEAF, modelItem.toString, schemaItem.asInstanceOf[DataType].typeName ));
        }
        else
        {
            System.err.println("Schema item not understood:");
            System.err.println("\tasString: " + modelItem.toString);
            System.err.println("\tclassAsString: " + modelItem.getClass.toString);
        }
    }
    return (vertices, edges);
}

def model2graphframe( modelDF:DataFrame ) : GraphFrame =
{
    val graphdata = model2graph_schema( modelDF.collect, modelDF.schema, 0 );

    val verticesDF = graphdata._1.toDF( "id", "value", "valueType" );
    val edgesDF = graphdata._2.toDF( "src", "dst", "key" );
 return GraphFrame( verticesDF, edgesDF );
   
}
val gf = model2graphframe( modelDF );
 
===================== Motif algorithm =========================================== 

val motifs = ngf.find("(a)-[e]->(b); (b)-[ea]->(c); (c)-[eb]->(d)").filter("e.key = 'classmm:Package'")
val esub =  motifs.select("eb.src", "eb.dst", "eb.key")
val gfSubPckDF = GraphFrame (gf.vertices, esub).dropIsolatedVertices()
val subEdgesPckDF = gfSubPckDF.edges.withColumn("key", regexp_extract($"key","""[:A-Za-z0-9]*$""",0))

val motifs = ngf.find("(a)-[e]->(b); (b)-[ea]->(c); (c)-[eb]->(d)").filter("e.key = 'classes'")
val esub =  motifs.select("eb.src", "eb.dst", "eb.key")
val gfSubClasDF = GraphFrame (gf.vertices, esub).dropIsolatedVertices()
val subEdgesClasDF = gfSubClasDF.edges.withColumn("key", regexp_extract($"key","""[:A-Za-z0-9]*$""",0))

val motifs = ngf.find("(a)-[e]->(b); (b)-[ea]->(c); (c)-[eb]->(d)").filter("e.key = 'att'")
val esub =  motifs.select("eb.src", "eb.dst", "eb.key")
val gfSubAttDF = GraphFrame (gf.vertices, esub).dropIsolatedVertices()

=========================motifs Female============================== 

val motifsf = gf.find("(a)-[e]->(b); (b)-[ea]->(c)").filter(("e.key != 'father' and e.key != 'sons' and ea.key != 'father' and ea.key != 'sons'"))
val edgesSubf = motifsf.where("(ea.key != '_firstName')").select($"ea.dst".alias("x")).join(motifsf).filter("(x = ea.src)").select("ea.*")
val edgesSubfr = motifsf.where("(ea.key != '_firstName')").select("ea.*")
val gfsubf = GraphFrame (gf.vertices, edgesSubf.union(edgesSubfr).distinct).dropIsolatedVertices()

=========================motifs Male============================== 

val motifsm = gf.find("(a)-[e]->(b); (b)-[ea]->(c)").filter(("e.key != 'mother' and e.key != 'daughters' and ea.key != 'mother' and ea.key != 'daughters'"))
val edgesSubm = motifsm.where("(ea.key != '_firstName')").select($"ea.dst".alias("x")).join(motifsm).filter("(x = ea.src)").select("ea.*")
val edgesSubmr = motifsm.where("(ea.key != '_firstName')").select("ea.*")
val gfsubm = GraphFrame (gf.vertices, edgesSubm.union(edgesSubmr).distinct).dropIsolatedVertices()

=================inDegrees and outDegrees algorithm ================================================

val inDeg = ngf.inDegrees
val in = inDeg.select("inDegree").groupBy("inDegree").count()
  in.show(false)

val outDeg = ngf.outDegrees
val out = outDeg.select("outDegree").groupBy("outDegree").count()
  out.show()
    
val in = inDeg.select("inDegree").groupBy("inDegree").count()
  in.show()

val outDeg = esub.outDegrees
val out = outDeg.select("outDegree").groupBy("outDegree").count()
  out.show()
