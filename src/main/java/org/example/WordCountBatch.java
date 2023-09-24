package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        String inPath = "/Users/jiale.he/IdeaProjects/flink-aspect-demo/src/main/resources/hello.txt";
        String outPath = "/Users/jiale.he/IdeaProjects/flink-aspect-demo/src/main/resources/output.txt";
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = executionEnvironment.readTextFile(inPath);
        DataSet<Tuple2<String, Integer>> dataSet = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
        dataSet.writeAsCsv(outPath, "\n", " ").setParallelism(1);
        executionEnvironment.execute("word count batch process");
    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) {
            for (String word : line.split(" ")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
