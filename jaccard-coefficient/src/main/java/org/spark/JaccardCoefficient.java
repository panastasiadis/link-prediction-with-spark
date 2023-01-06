package org.spark;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class JaccardCoefficient {

    private static final Pattern SPACE = Pattern.compile("[ \\t\\x0B\\f\\r]+");

    public static JavaPairRDD<String, Integer> findCommonNeighborsScores(JavaPairRDD<String, String> graphEdges, JavaPairRDD<String, Tuple2<String, String>> joinedEdges) {

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
        JavaPairRDD<String, Integer> unconnectedEdgeInstances = tempResults.subtract(graphEdges).mapToPair(s -> {
            return new Tuple2<String, Integer>(s._1() + " <-> " + s._2(), 1);
        });

        //1) reduceByKey
        //this is the final result containing pairs of (edge-of-unconnected-nodes, number-of-common-neighbors)
        return unconnectedEdgeInstances.reduceByKey((v1, v2) -> v1 + v2);
    }


    public static List<Tuple2<Double, String>> jaccardCoefficient(JavaRDD<String> lines, Integer numOfDisplayedScores) {

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


        //1) flatMapToPair
        // transform the edges to instances of (node, 1) where "1" corresponds to 1 neighbor
        //2) reduceByKey
        //create pairs (node, total neighbors)
        JavaPairRDD<String, Integer> nodesWithNeighborsCount = edges.mapToPair((s) -> {
            return (new Tuple2<>(s._1(), 1));
        }).reduceByKey((v1, v2) -> v1 + v2);

        //1. transform the pairs of type (common-neighbor, [node1, node2]) to pairs of (node1, node2)
        //2. exclude pairs of type (a, a) and keep only the distinct values of the result
        JavaPairRDD<String, String> tempResultsPart1 = joinedEdges.flatMapToPair(s -> {
            ArrayList<Tuple2<String, String>> arrayList = new ArrayList<>();
            if (Integer.parseInt(s._2()._1()) != Integer.parseInt(s._2()._2())) {
                arrayList.add(s._2());
            }
            return arrayList.iterator();
        }).distinct();

        //subtract
        //subtract the existing edges of the graph from the filtered remaining edges of the previous result ->
        //-> in order to keep the edges of unconnected nodes only
        //join
        //join the result with the rdd of pairs (node, total-neighbors). The outcome pairs contain both ->
        // -> (node1, (node2, total-neighbors-of-node1) and (node2, (node1, total-neighbors-of-node2)
        JavaPairRDD<String, Tuple2<String, Integer>> tempResultPart2 = tempResultsPart1.subtract(edges).join(nodesWithNeighborsCount);

        //1) mapToPair
        //we transform the previous result to pairs of (node1<->node2, total-neighbors-of-node1)
        //for the reverse edges we create (node1<->node2, total-neighbors-of-node2)
        //2) reduceByKey
        //we reduce the instances to the final result of pairs of (node1 <->node2, sum-of-their-neighbors)
        JavaPairRDD<String, Integer> totalNeighbors = tempResultPart2.mapToPair(s -> {
            if (Integer.parseInt(s._1()) < Integer.parseInt(s._2()._1())) {
                return new Tuple2<>(s._1() + " <-> " + s._2()._1(), s._2()._2());
            } else {
                return new Tuple2<>(s._2()._1() + " <-> " + s._1(), s._2()._2());
            }
        }).reduceByKey((v1, v2) -> v1 + v2);

        //find the common neighbors of the unconnected edges
        JavaPairRDD<String, Integer> commonNeighbors = findCommonNeighborsScores(edges, joinedEdges);

        //join the common neighbors result with the union of neighbors result
        //result is a set of pairs of type (unconnected-edge, (number-of-common-neighbors, sum-of-the-nodes-neighbors))
        JavaPairRDD<String, Tuple2<Integer, Integer>> jaccardCoefficientTempResult = commonNeighbors.join(totalNeighbors);

        //calculate the jaccard coefficient
        //result now is a set of pairs of type (unconnected-edge, jaccard-coefficient)
        JavaPairRDD<String, Double> jaccardCoefficientScores = jaccardCoefficientTempResult.mapValues(v -> (double) v._1() / (double) (v._2() - v._1()));

        //reverse the resulting tuples for the future sortByKey
        JavaPairRDD<Double, String> reversedTuples = jaccardCoefficientScores.mapToPair(s -> {
            return new Tuple2<>(s._2(), s._1());
        });

        //return the sorted result
        return reversedTuples.sortByKey(false).take(numOfDisplayedScores);
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: Arguments must be <inputpath> <number-of-top-scores-to-be-displayed>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JaccardCoefficient").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        //calculate the top jaccardCoefficient scores for every unconnected edge ->
        //-> of two nodes with at least one common neighbor
        List<Tuple2<Double, String>> jcScores = jaccardCoefficient(lines, Integer.parseInt(args[1]));

        //print the results
        for (Tuple2<Double, String> score : jcScores) {
            System.out.println(String.format("%.5f", score._1()) + ", " + score._2());
        }

        sc.stop();

    }

}
