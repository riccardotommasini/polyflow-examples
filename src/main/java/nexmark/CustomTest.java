package nexmark;

import nexmark.queries.custom.*;
import nexmark.queries.tablesaw.Query4;
import nexmark.utils.Query;

public class CustomTest {
    public static void main(String [] args){
        Query q = new Query8Custom();
        double sum_throughput=0;
        double sum_totalTime=0;
        double sum_spentParsing=0;
        for(int i = 0; i<10; i++){
            System.out.println(q.getClass().getName()+", iteration "+i);
            q.execute();
            sum_throughput+=q.getThroughput();
            sum_totalTime+=q.getTotalTime();
            sum_spentParsing+= q.getTimeSpentParsing();
        }
        double throughput = (sum_throughput/10);
        double totaltime = (sum_totalTime/10);
        double parsing = (sum_spentParsing/10);
        System.out.println(throughput);
        System.out.println(totaltime);
        System.out.println(parsing);

    }
}
