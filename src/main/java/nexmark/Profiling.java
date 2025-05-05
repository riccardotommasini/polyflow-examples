package nexmark;

import nexmark.queries.custom.Query1Custom;
import nexmark.queries.custom.Query2Custom;
import nexmark.queries.custom.Query6Custom;
import nexmark.utils.Query;

public class Profiling {

    public static void main(String [] args){
        int iterations = 1;
        int events = 1000000;
        Query q = new Query2Custom();
        double sum_throughput=0;
        double sum_totalTime=0;
        double sum_spentParsing=0;
        for(int i = 0; i<iterations; i++){
            System.out.println(q.getClass().getName()+", iteration "+i);
            q.execute();
            sum_throughput+=q.getThroughput();
            sum_totalTime+=q.getTotalTime();
            sum_spentParsing+= q.getTimeSpentParsing();
        }
        System.out.println(sum_throughput/iterations);
        System.out.println(sum_totalTime/iterations);
        System.out.println(sum_spentParsing/iterations);
    }
}
