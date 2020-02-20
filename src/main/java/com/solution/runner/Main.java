package com.solution.runner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class Main {

    //TODO to pass inputs as arguments
    public static void main(String args[]){

        SparkConf sparkConf = new SparkConf().setAppName("Tasks Scheduling")
                .setMaster("local").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        File taskfile = new File(Main.class.getClassLoader().getResource("input/task_ids.txt").getFile());
        JavaRDD<String> tasks = sc.textFile(taskfile.getPath());
        File relationsFile = new File(Main.class.getClassLoader().getResource("input/relations.txt").getFile());

        JavaPairRDD<Integer,Integer> relationsRDD = sc.textFile(relationsFile.getPath()).mapToPair(line -> {
            String[] vertices = line.split("->");
            return new Tuple2<>(Integer.valueOf(vertices[0]),Integer.valueOf(vertices[1]));
        });
        new Driver.DriverBuilder()
                .startNode(73)
                .endNode(36)
                .addtasksList(tasks.flatMap(content -> Arrays.asList(content.split(","))))
                .relations(relationsRDD)
                .withScheduling(new ScheduleImpl())
                .buildAndRun();
    }
}
