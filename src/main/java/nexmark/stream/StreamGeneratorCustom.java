package nexmark.stream;

import nexmark.customdatatypes.*;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamGeneratorCustom {
    private final Map<String, DataStream<Serializable>> activeStreams;

    private File f1 = new File(RowStreamGenerator.class.getResource("/events.txt").getPath());
    private Scanner s1;
    private final AtomicBoolean isStreaming;
    public double numEvents = 1000000;
    public double throughput;
    public double totalTime;
    public double timeSpentParsing;


    public StreamGeneratorCustom() {
        this.activeStreams = new HashMap<String, DataStream<Serializable>>();
        this.isStreaming = new AtomicBoolean(false);
        try {
            s1 = new Scanner(f1);
        }catch(FileNotFoundException ignored){}
    }


    public DataStream<Serializable> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            RowStream<Serializable> stream = new RowStream<>(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {

        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            long start = System.currentTimeMillis();
            timeSpentParsing = 0;
            totalTime = 0;
            long prev_ts = -1;
            while (this.isStreaming.get() && s1.hasNext()) {
                long parseStart = System.currentTimeMillis();
                String tuple = s1.nextLine();
                String[] valAndTs = tuple.split(",", 2);
                long ts = Long.parseLong(valAndTs[0]);
                prev_ts = ts;
                int i = 0;
                StringBuilder b = new StringBuilder();
                tuple = valAndTs[1];
                /*----Phase 1: parse the type of tuple (Auction, Person or Bid)----*/
                while(tuple.charAt(i)!= '{'){
                    b.append(tuple.charAt(i));
                    i++;
                }
                String name = b.toString();
                tuple = tuple.substring(i+1, tuple.length()-1);
                Serializable event;
                if(name.equals("Auction")){
                    event = new AuctionEvent(tuple, ts);
                }
                else if(name.equals("Person")){
                    event = new PersonEvent(tuple, ts);
                }
                else if(name.equals("Bid")){
                    event = new BidEvent(tuple, ts);
                }
                else{
                    throw new RuntimeException("Wrong event type during parsing");
                }
                timeSpentParsing+=System.currentTimeMillis()-parseStart;
                activeStreams.get(name).put(event, ts);

            }
            stopStreaming();
            totalTime = System.currentTimeMillis()-start;
            throughput = numEvents/totalTime;

        }
    }



    public void stopStreaming() {
        this.isStreaming.set(false);
    }
}

