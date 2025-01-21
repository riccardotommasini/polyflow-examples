package nexmark.content;

import nexmark.customdatatypes.tablesaw.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;
import tech.tablesaw.api.Table;

import java.util.function.BiFunction;
import java.util.function.Function;

public class ExpiredAuctionContentFactory implements ContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Table> {

    Table emptyContent;
    Function<TimestampedElement<Table>, Table> f1;
    BiFunction<Table, Table, Table> sumR;

    public ExpiredAuctionContentFactory(Table emptyContent, Function<TimestampedElement<Table>, Table> f1, BiFunction<Table, Table, Table> sumR){
        this.emptyContent = emptyContent;
        this.f1 = f1;
        this.sumR = sumR;
    }
    @Override
    public Content<TimestampedElement<Table>, TimestampedElement<Table>, Table> createEmpty() {
        return new EmptyContent<>(emptyContent);
    }

    @Override
    public Content<TimestampedElement<Table>, TimestampedElement<Table>, Table> create() {
        return new ExpiredAuctionContent(emptyContent, f1, sumR);
    }
}

