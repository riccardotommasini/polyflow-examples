package nexmark.content;

import nexmark.customdatatypes.TestTimestampedRow;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;
import tech.tablesaw.api.Table;

public class LogicalSlidingContentFactory implements ContentFactory<TestTimestampedRow, TestTimestampedRow, Table> {

    Table emptyContent;
    long windowSize;
    public LogicalSlidingContentFactory(Table emptyContent, long windowSize){
        this.emptyContent = emptyContent;
        this.windowSize = windowSize;
    }
    @Override
    public Content<TestTimestampedRow, TestTimestampedRow, Table> createEmpty() {
        return new EmptyContent<>(emptyContent);
    }

    @Override
    public Content<TestTimestampedRow, TestTimestampedRow, Table> create() {
        return new LogicalSlidingContent(emptyContent, windowSize);
    }
}
