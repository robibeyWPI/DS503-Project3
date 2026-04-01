import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Problem2 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Problem1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        JavaRDD<String> lines = spark.read()
                .textFile("hdfs:///user/cs585/project3/input/P_project3.txt")
                .javaRDD();

        JavaRDD<Tuple2<Integer, Integer>> points = lines.map(line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(
                    Integer.parseInt(parts[0]),
                    Integer.parseInt(parts[1])
            );
        });

        JavaPairRDD<Integer, Integer> cellPairs = points.mapToPair(p -> {
            int x = p._1;
            int y = p._2;

            int row = (y - 1) / 20; // Size 20 x 20
            int col = (x - 1) / 20;

            int cellID = row * 500 + col; // 500 x 500 grid

            return new Tuple2<>(cellID, 1);
        });

        JavaPairRDD<Integer, Integer> cellCounts = cellPairs.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, Iterable<Integer>> neighborsRDD = cellCounts.mapToPair(cell -> {
            int cellID = cell._1;
            int row = cellID / 500;
            int col = cellID % 500;

            List<Integer> neighbors = new ArrayList<>();

            for (int dr = -1; dr <= 1; dr++) {
                for (int dc = -1; dc <= 1; dc++) {
                    int neighborRow = row + dr;
                    int neighborCol = col + dc;

                    if (neighborRow >= 0 && neighborRow < 500 && neighborCol >=0 && neighborCol < 500) {
                        neighbors.add(neighborRow * 500 + neighborCol);
                    }
                }
            }
            return new Tuple2<>(cellID, neighbors);
        });

        JavaPairRDD<Integer, Integer> expanded = neighborsRDD.flatMapToPair(cell -> {
            List<Tuple2<Integer, Integer>> result = new ArrayList<>();

            int cellID = cell._1;

            for (int neighbor : cell._2) {
                result.add(new Tuple2<>(neighbor, cellID));
            }
            return result.iterator();
        });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joined = expanded.join(cellCounts);

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> cellNeighborStats = joined.mapToPair(pair -> {
            int cellID = pair._2._1;
            int neighborCount = pair._2._2;

            return new Tuple2<>(cellID, new Tuple2<>(neighborCount, 1));
        });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> reduced = cellNeighborStats.reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        JavaPairRDD<Integer, Float> relativeDensity = reduced.join(cellCounts).mapToPair(pair -> {
            int cellID = pair._1;
            int neighborSum = pair._2._1._1;
            int neighborCount = pair._2._1._2;
            int cellCount = pair._2._2;;

            float avgNeighbors = (float) neighborSum / neighborCount;
            float I = cellCount / avgNeighbors;

            return new Tuple2<>(cellID, I);
        });

        List<Tuple2<Integer, Float>> top50 = relativeDensity.takeOrdered(50, new SerializableComparator());

        for (Tuple2<Integer, Float> t : top50) {
            System.out.println(t._1 + " -> " + t._2);
        }
    }

    public static class SerializableComparator implements Comparator<Tuple2<Integer, Float>>, Serializable {
        public int compare(Tuple2<Integer, Float> a, Tuple2<Integer, Float> b) {
            return Float.compare(b._2, a._2);
        }
    }
}
