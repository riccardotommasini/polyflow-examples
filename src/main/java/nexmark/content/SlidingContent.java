package nexmark.content;

import nexmark.customdatatypes.TestTimestampedRow;
import nexmark.customdatatypes.TimestampedRow;
import org.streamreasoning.polyflow.api.secret.content.Content;
import tech.tablesaw.api.Table;

import java.util.LinkedList;
import java.util.Queue;

public class SlidingContent implements Content<TestTimestampedRow, TestTimestampedRow, Table> {

    Queue<TestTimestampedRow> content = new LinkedList<>();
    Table emptyContent;
    long windowSize;

    public SlidingContent(Table emptyContent, long windowSize){
        this.emptyContent = emptyContent;
        this.windowSize = windowSize;
    }
    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(TestTimestampedRow timestampedRow) {
        content.offer(timestampedRow);
        while(!content.isEmpty() && timestampedRow.getTimestamp() - windowSize >= content.peek().getTimestamp()){
            content.poll();
        }
    }

    @Override
    public Table coalesce() {
        return content.stream().map(x->x.getRow()).reduce(emptyContent, (r1, r2) -> r1.isEmpty() ? r2 : r1.append(r2));
    }
}
