package nexmark.stream;

import nexmark.customdatatypes.TestTimestampedRow;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Tuple;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamGenerator {
    private static final Long TIMEOUT = 1000l;
    private final Map<String, DataStream<TestTimestampedRow>> activeStreams;

    private File f1 = new File(RowStreamGenerator.class.getResource("/events.txt").getPath());
    private Scanner s1;
    private final AtomicBoolean isStreaming;


    public StreamGenerator() {
        this.activeStreams = new HashMap<String, DataStream<TestTimestampedRow>>();
        this.isStreaming = new AtomicBoolean(false);
        try {
            s1 = new Scanner(f1);
        }catch(FileNotFoundException ignored){}
    }


    public DataStream<TestTimestampedRow> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            RowStream<TestTimestampedRow> stream = new RowStream<>(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task = () -> {
                long prev_ts = -1;
                while (this.isStreaming.get() && s1.hasNext()) {
                    String tuple = s1.nextLine();
                    String[] valAndTs = tuple.split(",", 2);
                    long ts = Long.parseLong(valAndTs[0]);
                    if(prev_ts == -1)
                        prev_ts = ts;

                    else if(ts-prev_ts > 0){
                        try {
                            Thread.sleep(ts - prev_ts);
                        }catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        prev_ts = ts;
                    }
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
                    activeStreams.get(name).put(new TestTimestampedRow(parse(tuple, name), ts), ts);
                    /*try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }*/
                }
                stopStreaming();

            };


            Thread thread = new Thread(task);
            thread.start();
        }
    }


    private Table parse(String value, String name){
        Table t = Table.create();
        t.setName(name); //useful?
        /* Split the remaining string in values with shape 'NameOfField=ValueOfField' */
        String[] fields = value.split(", ");

        for(String field : fields){
            String[] keyval = field.split("=");
            Column c;
            if(keyval[1].charAt(0) == '\''){
                //remove the ' delimiters from the string
                 c = StringColumn.create(keyval[0],keyval[1].substring(1, keyval[1].length()-1) );
            }
            else if (!keyval[0].equals("dateTime") && !keyval[0].equals("expires")){
                 c = LongColumn.create(keyval[0], Long.parseLong(keyval[1]));
            }
            else{
                 c = InstantColumn.create(keyval[0], Instant.parse(keyval[1]));
            }
            t.addColumns(c);

        }
        return t;

    }


    public void stopStreaming() {
        this.isStreaming.set(false);
    }
}
