package org.spark;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class AdamicAdar {

    private static final Pattern SPACE = Pattern.compile("[ \\t\\x0B\\f\\r]+");

    public static List<Tuple2<Double, String>> adamicAdar(JavaRDD<String> lines, Integer numOfDisplayedScores) {

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

        edges.cache();

        //1) mapToPair
        // transform the edges to instances of (node, 1) where "1" corresponds to 1 neighbor
        //2) reduceByKey
        //create pairs (node, total neighbors)
        JavaPairRDD<String, Integer> nodesWithNeighborsCount = edges.mapToPair((s) -> {
            return (new Tuple2<>(s._1(), 1));
        }).reduceByKey((v1, v2) -> v1 + v2);

        //1)
        //join the edges with a copy of themselves to create edges ->
        //-> of nodes connected to the neighbors of their neighbors
        // output is a set of pairs of (common-neighbor, [node1, node2])
        //2)
        //this set of pairs also contain:
        // 1. -> pairs of nodes with the type of (a, a)
        // 2. -> all the reverse edges of the resulting edges e.g. both (a, b) and (b, a)
        // 3. -> pairs of already connected nodes (due to exsiting connections of nodes with common neighbors)
        JavaPairRDD<String, Tuple2<String, String>> joinedEdges = edges.join(edges);

        //caching
        joinedEdges.cache();

        //1) flatMapToPair + distinct
        //remove the reversed edges and the edges of type (a, a) and keep only the distinct values of the result
        //2) subtract
        //subtract the existing edges from the new filtered result in order to keep the unconnected edges only
        JavaPairRDD<String, String> unconnectedEdgesRaw = joinedEdges.flatMapToPair(s -> {
            ArrayList<Tuple2<String, String>> arrayList = new ArrayList<>();
            if (Integer.parseInt(s._2()._1()) < Integer.parseInt(s._2()._2())) {
                arrayList.add(s._2());
            }
            return arrayList.iterator();
        }).distinct().subtract(edges);


        //write the unconnected edges in the form of (node1 node2, -1) to allow the future join action (explained below)
        //-1 is dummy info
        JavaPairRDD<String, Integer> unconnectedEdges = unconnectedEdgesRaw
                .mapToPair(s -> new Tuple2<>(s._1() + " <-> " + s._2(), -1));

        //joinedEdges contain pairs of type (common-neighbor, (node1, node2))
        //filter again the joinedEdges RDD by removing pairs of (a,a) and the reverse edges
        JavaPairRDD<String, Tuple2<String, String>> filteredJoinedEdges = joinedEdges.filter(s -> {
            return Integer.parseInt(s._2()._1()) < Integer.parseInt(s._2()._2());
        });

        //1) join
        //join the filteredJoinedEdges with the nodesWithNeighborsCount to create a set of pairs like below:
        //for an edge (a, b) with common neighbors n1, n2 we create pairs of (n1, [(a,b), n1-total-neighbors]),
        // (n2, [(a, b), n2-total-neighbors])
        //2) mapToPair
        // create a new set of pairs with type (node1 node2, common-neighbor-total-neighbors)
        // for the above example it will produce (a b, n1-total-neighbors), (a b, n2-total-neighbors)
        //groupByKey
        // groupByKey will transform the flatMapToPair output to pairs of type ->
        // -> (a b, [n1-total-neighbors, n2-total-neighbors])
        JavaPairRDD<String, Iterable<Integer>> adamicAdarParameters = filteredJoinedEdges.join(nodesWithNeighborsCount)
                .mapToPair(s -> {
                    return new Tuple2<>(s._2()._1()._1() + " <-> " + s._2()._1()._2(), s._2()._2());
                }).groupByKey();

        //now we are ready to compute adamic adar scores!
        //1) join
        // the join with the previously computed RDD of unconnected edges of type (unconnected-edge, -1) ->
        //-> will let us keep the unconnected edges only in the final result
        //2) mapValues
        //in the mapValues we compute the AdamicAdar Scores
        JavaPairRDD<String, Double> adamicAdarScores = adamicAdarParameters.join(unconnectedEdges).mapValues(v -> {
            Iterable<Integer> countsOfNeighborsOfCommonNeighbors = v._1();
            double adamicAdar = 0;
            for (Integer n : countsOfNeighborsOfCommonNeighbors) {
                adamicAdar += (1 / Math.log10(n));
            }
            return adamicAdar;
        });

        //reverse the resulting tuples for the sortByKey below
        JavaPairRDD<Double, String> reversedTuples = adamicAdarScores.mapToPair(s -> {
            return new Tuple2<>(s._2(), s._1());
        });

        //return the sorted result
        return reversedTuples.sortByKey(false).take(numOfDisplayedScores);
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: AdamicAdar <inputpath> <outputpath> <number_of_iterations>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("AdamicAdar").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        //calculate adamic adar scores for every unconnected edge of two nodes with at least one common neighbor
        List<Tuple2<Double, String>> aaScores = adamicAdar(lines, Integer.parseInt(args[1]));

        //print the result
        for (Tuple2<Double, String> score : aaScores) {
            System.out.println(String.format("%.5f", score._1()) + ", " + score._2());
        }

        sc.stop();

    }

}
