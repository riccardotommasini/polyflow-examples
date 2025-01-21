package nexmark.content.unused;

import nexmark.customdatatypes.tablesaw.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;
import tech.tablesaw.api.Table;

public class Q5SlidingContentFactory implements ContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Long> {

    long windowSize;

    public Q5SlidingContentFactory(long windowSize){
        this.windowSize = windowSize;
    }
    @Override
    public Content<TimestampedElement<Table>, TimestampedElement<Table>, Long> createEmpty() {
        return new EmptyContent<>(0L);
    }

    @Override
    public Content<TimestampedElement<Table>, TimestampedElement<Table>, Long> create() {
        return new Q5SlidingContent(windowSize);
    }
}
