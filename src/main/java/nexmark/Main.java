package nexmark;

import nexmark.queries.*;
import nexmark.utils.Query;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {


    public static void main(String[] args) throws IOException {

        int iterations = 10;

        List<Query> queries = new ArrayList<>();
        List<Double> throughput = new ArrayList<>();
        List<Double> totalTime = new ArrayList<>();
        List<Double> spentParsing = new ArrayList<>();
        String filePath = "src/main/resources/results.txt";

        // Create a File object
        File file = new File(filePath);
        file.createNewFile();
        FileWriter writer = new FileWriter(file);

        queries.add(new Query1());
        queries.add(new Query2());
        queries.add(new Query3());
        queries.add(new Query4());
        queries.add(new Query5());
        queries.add(new Query6());
        queries.add(new Query7());
        queries.add(new Query8());
        long start = System.currentTimeMillis();
        for(Query q : queries){
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
            throughput.add(sum_throughput/iterations);
            totalTime.add(sum_totalTime/iterations);
            spentParsing.add(sum_spentParsing/iterations);
        }
        for(int i = 0; i<queries.size(); i++){
            writer.write("\nQuery "+(i+1)+":");
            writer.write("\nAverage Total Time: " +totalTime.get(i) +" ms");
            writer.write("\nAverage Throughput: " +throughput.get(i)+" events/ms");
            writer.write("\nAverage Time Spent Parsing: " +spentParsing.get(i)+ " ms");
        }
        writer.close();
        System.out.println("Total time for one iteration: "+(System.currentTimeMillis()-start));


    }
}
