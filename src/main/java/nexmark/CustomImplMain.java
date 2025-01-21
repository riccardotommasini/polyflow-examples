package nexmark;

import com.opencsv.CSVWriter;
import nexmark.queries.custom.*;
import nexmark.queries.tablesaw.*;
import nexmark.utils.Query;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomImplMain {
    public static void main(String [] args) throws IOException {

        int iterations = 10;
        int events = 1000000;

        List<Query> queries = new ArrayList<>();
        List<Double> throughput = new ArrayList<>();
        List<Double> totalTime = new ArrayList<>();
        List<Double> spentParsing = new ArrayList<>();
        String filePath = "src/main/resources/resultsCustom.txt";

        File file = new File(filePath);
        file.createNewFile();
        CSVWriter writer = new CSVWriter(new FileWriter(file, true));
        String[] firstRow = new String[]{"Experiment-Name", "Throughput(events/ms)", "InputSize", "MillisecondsPassed", "ParsingTime(ms)"};
        writer.writeNext(firstRow);
        writer.flush();

        queries.add(new Query1Custom());
        queries.add(new Query2Custom());
        queries.add(new Query3Custom());
        queries.add(new Query4Custom());
        queries.add(new Query5Custom());
        queries.add(new Query6Custom());
        queries.add(new Query7Custom());
        queries.add(new Query8Custom());
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
            String[] row = new String[]{"query-"+(i+1), String.valueOf(throughput.get(i)), String.valueOf(events), String.valueOf(totalTime.get(i)), String.valueOf(spentParsing.get(i))};
            writer.writeNext(row);
            try {
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        writer.close();
        System.out.println("Total time: "+(System.currentTimeMillis()-start));


    }
}
