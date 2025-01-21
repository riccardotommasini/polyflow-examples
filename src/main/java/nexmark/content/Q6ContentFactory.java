package nexmark.content;

import nexmark.customdatatypes.tablesaw.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;
import tech.tablesaw.api.Table;

import java.util.function.BiFunction;
import java.util.function.Function;

/*
A mix of the ExpiredAuctionContent used for Query 4 with a physical sliding window logic
 */
public class Q6ContentFactory implements ContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Table> {

    Table emptyContent;
    Function<TimestampedElement<Table>, Table> f1;
    BiFunction<Table, Table, Table> sumR;
    int windowSize;

    public Q6ContentFactory(Table emptyContent, Function<TimestampedElement<Table>, Table> f1, BiFunction<Table, Table, Table> sumR, int windowSize){
        this.emptyContent = emptyContent;
        this.f1 = f1;
        this.sumR = sumR;
        this.windowSize = windowSize;
    }
    @Override
    public Content<TimestampedElement<Table>, TimestampedElement<Table>, Table> createEmpty() {
        return new EmptyContent<>(emptyContent);
    }

    @Override
    public Content<TimestampedElement<Table>, TimestampedElement<Table>, Table> create() {
        return new Q6Content(emptyContent, f1, sumR, windowSize);
    }
}


