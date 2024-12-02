package nexmark.content;

import nexmark.customdatatypes.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Function;

/*
A mix of the ExpiredAuctionContent used for Query 4 with a physical sliding window logic
 */
public class Q6Content implements Content<TimestampedElement<Table>, TimestampedElement<Table>, Table> {

    Table emptyContent;
    List<TimestampedElement<Table>> validAuctions = new ArrayList<>();
    Queue<TimestampedElement<Table>> expiredAuctions = new LinkedList<>();
    int windowSize;
    Function<TimestampedElement<Table>, Table> f1;
    BiFunction<Table, Table, Table> sumR;



    public Q6Content(Table emptyContent, Function<TimestampedElement<Table>, Table> f1, BiFunction<Table, Table, Table> sumR, int windowSize){
        this.emptyContent = emptyContent;
        this.f1 = f1;
        this.sumR = sumR;
        this.windowSize = windowSize;
    }

    @Override
    public int size() {
        return expiredAuctions.size();
    }

    @Override
    public void add(TimestampedElement<Table> tableTimestampedElement) {
        long timestamp = tableTimestampedElement.getTimestamp();
        for(int i = validAuctions.size()-1; i>=0; i--){
            if(validAuctions.get(i).getElement().instantColumn("expires").get(0).toEpochMilli() < timestamp){
                expiredAuctions.offer(validAuctions.get(i));
                validAuctions.remove(i);
                while(!expiredAuctions.isEmpty() && expiredAuctions.size() > windowSize)
                    expiredAuctions.poll();
            }
        }
        validAuctions.add(tableTimestampedElement);
    }

    @Override
    public Table coalesce() {
        Table res = expiredAuctions.stream().map(a->f1.apply(a)).reduce(emptyContent, (t1, t2)->sumR.apply(t1, t2));
        return res;
    }
}
