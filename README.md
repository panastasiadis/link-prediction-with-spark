# link-prediction-with-spark

This guide assumes you have an existing installation of Spark and Java OpenJDK on your Linux device.

TIn order to avoid Java heap errors, spark commands include the option `--driver-memory 4g` to give the Spark driver enough memory.

## Compile

To compile the project's apps, simply open a terminal inside the project's root directory.
Run `cd ./adamic-adar`, `cd ./common-neighbors/` or `cd ./jaccard-coefficient/` 
to navigate into the desired app. Then simply run:

```
./mvnw install
```
If you want to undo the previous command, run:
```
./mvnw clean
```

## Execute

After compiling an app, you can execute it by running the following, **inside its directory**.

> _NOTE_: Replace **< input-file-directory >** with the absolute path of your input file's directory. You can use the files inside the given input directory.

> _NOTE_: Replace **< number-of-the-top-results-to-be-displayed >** with a number. E.g. 500 will produce the top-500 results.

For Common Neighbors: 
```
spark-submit --class org.spark.CommonNeighbors --driver-memory 4g \
./target/commonneighbors-0.1.jar <input-file-directory> <number-of-the-top-results-to-be-displayed>
```
For Jaccard Coefficient: 
```
spark-submit --class org.spark.JaccardCoefficient --driver-memory 4g \
./target/jaccardcoefficient-0.1.jar <input-file-directory> <number-of-the-top-results-to-be-displayed> 
```
For Adamic/Adar:
```
spark-submit --class org.spark.AdamicAdar --driver-memory 4g \
./target/adamicadar-0.1.jar <input-file-directory> <number-of-the-top-results-to-be-displayed> 
```
## Input

Each app takes as input a text file, containing an undirected graph.
* Lines starting with '#' are considered comments.
* Every edge of the graph is described in one line by two integers (the nodes) separated with space.
* Nodes must be described only as integers.
* Graph must be undirected, so if for example "1 2" is an existing edge of the graph, "2 1" must also be included.

Below is a demonstration of a graph created by using the structure described above.
```
# comment1
# comment2
2 1 
1 3 
4 2 
3 4 
5 3 
4 5 
1 2 
3 1 
2 4 
4 3 
3 5 
5 4 
```

## Ready-to-run Examples

You can test the apps by using the existing graph files from the **input** directory.

Inside the directory, there are two graph files from the snap.stanford.edu/data and a file called small_input containing the example described above.
* cA-AstroPh is taken from https://snap.stanford.edu/data/ca-AstroPh.html
* cA-CondMat is taken from https://snap.stanford.edu/data/ca-CondMat.html

### Example
To test the apps with the **ca-AstroPh** dataset and display the **top-5000** results, assuming you are in the project's root directory without having compiled anything, you can do:

For Common Neighbors:
```
cd common-neighbors
./mvnw install
spark-submit --class org.spark.CommonNeighbors --driver-memory 4g ./target/commonneighbors-0.1.jar  "$(realpath ../input/ca-AstroPh.txt)" 5000
```
For Jaccard Coefficient:
```
cd jaccard-coefficient
./mvnw install
spark-submit --class org.spark.JaccardCoefficient --driver-memory 4g ./target/jaccardcoefficient-0.1.jar "$(realpath ../input/ca-AstroPh.txt)" 5000
```
For Adamic/Adar:
```
cd adamic-adar
./mvnw install
spark-submit --class org.spark.AdamicAdar --driver-memory 4g ./target/adamicadar-0.1.jar "$(realpath ../input/ca-AstroPh.txt)" 5000
```
_Enjoy the results!_



