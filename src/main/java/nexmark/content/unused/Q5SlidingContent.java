package nexmark.content.unused;

import nexmark.customdatatypes.tablesaw.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import tech.tablesaw.api.Table;

import java.util.LinkedList;
import java.util.Queue;

public class Q5SlidingContent implements Content<TimestampedElement<Table>, TimestampedElement<Table>, Long> {

    Queue<TimestampedElement<Table>> content = new LinkedList<>();
    long windowSize;

    public Q5SlidingContent(long windowSize){
        this.windowSize = windowSize;
    }
    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(TimestampedElement<Table> timestampedElement) {
        if(timestampedElement.getElement() != null) //nullity check useful to filter out "empty" events that just make the window slide
            content.offer(timestampedElement);

        while(!content.isEmpty() && timestampedElement.getTimestamp() - windowSize >= content.peek().getTimestamp()){
            content.poll();
        }
    }

    @Override
    public Long coalesce() {
        return (long)content.size();
    }
}
