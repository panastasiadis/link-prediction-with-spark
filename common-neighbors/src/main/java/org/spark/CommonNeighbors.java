package org.spark;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CommonNeighbors {

    private static final Pattern SPACE = Pattern.compile("[ \\t\\x0B\\f\\r]+");

    public static List<Tuple2<Integer, String>> commonNeighbors(JavaRDD<String> lines, Integer numOfDisplayedScores) {

        //create an RDD of edges containing both (a, b) and (b, a)
        //also ignore lines with comments
        JavaPairRDD<String, String> edges = lines.flatMapToPair(s -> {
            String[] tokens = s.split(SPACE.pattern());
            ArrayList<Tuple2<String, String>> arrayList = new ArrayList<>();
            if (!s.contains("#")) {
                arrayList.add(new Tuple2<>(tokens[0], tokens[1]));
            }
            return arrayList.iterator();
        });

        //cache the result
        edges.cache();

        //1)
        //join the edges with a copy of themselves to create edges ->
        //-> of nodes connected to the neighbors of their neighbors
        // output is a set of pairs of (common-neighbor, [node1, node2])
        //2)
        //this is not yet the desired outcome because the result will also contain:
        // 1. -> pairs of nodes with the type of (a, a)
        // 2. -> all the reverse edges of the resulting edges e.g. both (a, b) and (b, a)
        // 3. -> pairs of already connected nodes (due to exsiting connections of nodes with common neighbors)
        JavaPairRDD<String, Tuple2<String, String>> joinedEdges = edges.join(edges);

        //filter the previous result by removing all the (a, a) pairs
        //remove also the reverse edges by keeping only (a,b) where a < b
        JavaPairRDD<String, String> tempResults = joinedEdges.flatMapToPair(s -> {
            ArrayList<Tuple2<String, String>> arrayList = new ArrayList<>();
            if (Integer.parseInt(s._2()._1()) < Integer.parseInt(s._2()._2())) {
                arrayList.add(s._2());
            }
            return arrayList.iterator();
        });

        //1) subtract
        //subtract the existing edges of the graph from the filtered remaining edges of the previous result ->
        //-> in order to keep the edges of unconnected nodes only
        //2) mapToPair
        // every instance of a pair of unconnected nodes is equal to a common neighbor of them
        // Because of that, map every instance with value "1" for the future reduction
        //3)
        // result will now contain pairs of (a<->b, 1)
        JavaPairRDD<String, Integer> unconnectedEdgeInstances = tempResults.subtract(edges).mapToPair(s -> {
            return new Tuple2<String, Integer>(s._1() + " <-> " + s._2(), 1);
        });

        //1) reduceByKey
        //this is the final result containing pairs of (edge-of-unconnected-nodes, number-of-common-neighbors)
        JavaPairRDD<String, Integer> scores = unconnectedEdgeInstances.reduceByKey((v1, v2) -> v1 + v2);

        //reverse the resulting tuples a for the future sortByKey
        JavaPairRDD<Integer, String> reversedTuples = scores.mapToPair(s -> {
            return new Tuple2<>(s._2(), s._1());
        });

        //return the top scores of the sorted results
        //numOfDisplayedScores defines the number of scores to return
        return reversedTuples.sortByKey(false).take(numOfDisplayedScores);
    }


    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: Arguments must be <inputpath> <number-of-top-scores-to-be-displayed>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("Common Neighbors").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        //calculate common neighbors metric
        List<Tuple2<Integer, String>> cnScores = commonNeighbors(lines, Integer.parseInt(args[1]));

        //print the result
        for (Tuple2<Integer, String> score : cnScores) {
            System.out.println(score);
        }

        sc.stop();

    }

}
