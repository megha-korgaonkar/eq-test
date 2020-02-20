package com.solution.runner;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.LinkedList;

public class Driver {

    private JavaPairRDD<Integer,Integer> relations;

    private JavaRDD<String> tasksList;

    private Integer startNode;

    private Integer endNode;

    public JavaPairRDD<Integer,Integer> getRelations() {
        return relations;
    }

    public JavaRDD<String> getTasksList() {
        return tasksList;
    }

    public Integer getStartNode() {
        return startNode;
    }

    public Integer getEndNode() {
        return endNode;
    }




    private Driver(DriverBuilder driverBuilder){
        this.startNode = driverBuilder.startNode;
        this.endNode   = driverBuilder.endNode;
        this.relations = driverBuilder.relations;
        this.tasksList = driverBuilder.tasksList;
    }




    public static class DriverBuilder {
        private JavaPairRDD<Integer,Integer> relations;

        private JavaRDD<String> tasksList;

        private Integer startNode;

        private Integer endNode;

        public Scheduling getScheduling() {
            return scheduling;
        }

        private Scheduling scheduling;

        DriverBuilder addtasksList(JavaRDD<String> tasksList){
            this.tasksList = tasksList;
            return this;
        }

        DriverBuilder relations(JavaPairRDD<Integer,Integer> relations){
            this.relations = relations;
            return this;
        }

        DriverBuilder startNode(Integer startNode){
            this.startNode = startNode;
            return this;
        }
        DriverBuilder endNode(Integer endNode){
            this.endNode = endNode;
            return this;
        }

        DriverBuilder withScheduling(Scheduling scheduling){
            this.scheduling = scheduling;
            return this;
        }

        public Driver buildAndRun(){
            Driver driver = new Driver(this);
            this.scheduling.schedule(driver);
            return driver;
        }

    }


}