package nexmark.content.unused;

import nexmark.customdatatypes.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;
import tech.tablesaw.api.Table;

import java.util.List;
import java.util.function.Function;

public class SynContainerContentFactory implements ContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, List<Long>> {

    private Function<TimestampedElement<Table>, Long> keyFromI;
    ContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Long> internalContentFactory;

    public SynContainerContentFactory(Function<TimestampedElement<Table>, Long> keyFromI ,
                                      ContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Long> internalContentFactory ){
        this.keyFromI = keyFromI;
        this.internalContentFactory = internalContentFactory;
    }

    @Override
    public Content<TimestampedElement<Table>, TimestampedElement<Table>, List<Long>> createEmpty() {
        return new EmptyContent<>(List.of(0L));
    }

    @Override
    public Content<TimestampedElement<Table>, TimestampedElement<Table>, List<Long>> create() {
        return new SynContainerContent(keyFromI, internalContentFactory);
    }
}
