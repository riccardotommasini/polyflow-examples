package nexmark;

import nexmark.queries.*;
import nexmark.utils.Query;

import java.util.ArrayList;
import java.util.List;

public class Main {


    public static void main(String[] args) {

        int iterations = 10;

        List<Query> queries = new ArrayList<>();
        List<Double> throughput = new ArrayList<>();
        List<Double> totalTime = new ArrayList<>();
        List<Double> spentParsing = new ArrayList<>();

        /*queries.add(new Query1());
        queries.add(new Query2());
        queries.add(new Query3());
        queries.add(new Query4());*/
       queries.add(new Query5());
        /*queries.add(new Query6());
        queries.add(new Query7());
        queries.add(new Query8());*/

        for(Query q : queries){
            double sum_throughput=0;
            double sum_totalTime=0;
            double sum_spentParsing=0;
            for(int i = 0; i<iterations; i++){
                q.execute();
                sum_throughput+=q.getThroughput();
                sum_totalTime+=q.getTotalTime();
                sum_spentParsing+= q.getTimeSpentParsing();
            }
            throughput.add(sum_throughput/iterations);
            totalTime.add(sum_totalTime/iterations);
            spentParsing.add(sum_spentParsing/iterations);
        }
        for(int i = 0; i<queries.size(); i++){
            System.out.println("Query "+(i+1)+":");
            System.out.println("Average Total Time: " +totalTime.get(i) +" ms");
            System.out.println("Average Throughput: " +throughput.get(i)+" events/ms");
            System.out.println("Average Time Spent Parsing: " +spentParsing.get(i)+ " ms");
            System.out.println(" ");
        }


    }
}
