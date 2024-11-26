package nexmark.content;

import nexmark.customdatatypes.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ExpiredAuctionContent implements Content<TimestampedElement<Table>, TimestampedElement<Table>, Table> {

    Table emptyContent;
    List<TimestampedElement<Table>> validAuctions = new ArrayList<>();
    List<TimestampedElement<Table>> expiredAuctions = new ArrayList<>();

    Function<TimestampedElement<Table>, Table> f1;
    BiFunction<Table, Table, Table> sumR;



    public ExpiredAuctionContent(Table emptyContent, Function<TimestampedElement<Table>, Table> f1, BiFunction<Table, Table, Table> sumR){
        this.emptyContent = emptyContent;
        this.f1 = f1;
        this.sumR = sumR;
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
                expiredAuctions.add(validAuctions.get(i));
                validAuctions.remove(i);
            }
        }
        validAuctions.add(tableTimestampedElement);
    }

    @Override
    public Table coalesce() {
        Table res = expiredAuctions.stream().map(a->f1.apply(a)).reduce(emptyContent, (t1, t2)->sumR.apply(t1, t2));
        expiredAuctions = new ArrayList<>();
        return res;
    }
}

