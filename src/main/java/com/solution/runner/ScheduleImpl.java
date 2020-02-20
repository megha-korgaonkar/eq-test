package com.solution.runner;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ScheduleImpl extends Scheduling implements Serializable {
    private static HashMap<Integer, LinkedList<Integer>> relations;

    @Override
    void schedule(Driver driver) {
        relations = new HashMap<>();
        driver.getRelations().foreach(rec-> { if(relations.containsKey(rec._1)){
            relations.get(rec._1).add(rec._2);
        }else{
            LinkedList<Integer> linkedList = new LinkedList<>();
            linkedList.add(rec._2);
            relations.put(rec._1,linkedList );
        }});

        Map<Integer,Boolean> visited = buildVisitedMap(driver);

        Stack stack = new Stack();
        for (Map.Entry<Integer,Boolean> entry : visited.entrySet()) {
            if (entry.getValue() == false)
                computeTaskSequence(driver.getStartNode(),driver.getEndNode(), visited, stack);
        }
        while (stack.empty()==false)
            System.out.print(stack.pop() + " ");
    }

    private Map<Integer, Boolean> buildVisitedMap(Driver driver) {
        Map<Integer,Boolean> visited = new LinkedHashMap<>();
        visited.put(driver.getEndNode(),false);
        visited.putAll(driver.getTasksList().collect().stream().filter(x-> !x.equals(driver.getEndNode())).collect(Collectors.toMap(x-> Integer.valueOf(x),(a)->false)));
        return visited;
    }

    private void computeTaskSequence(int startNode, int endNode, Map<Integer,Boolean> visited,
                             Stack stack) {
        visited.put(startNode,true);

        if (! (stack.contains(startNode) && stack.contains(endNode))) {
            if (startNode != endNode) {
                if (relations.containsKey(startNode)) {
                    for (Integer vertex : relations.get(startNode)) {
                        if (!visited.get(vertex))
                            computeTaskSequence(vertex, endNode, visited, stack);
                    }
                }
            }
            stack.push(new Integer(startNode));
        }
    }
}
