package nexmark.content;

import nexmark.customdatatypes.TestTimestampedRow;
import nexmark.customdatatypes.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import tech.tablesaw.api.Table;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Function;

public class LogicalSlidingContent<I, R> implements Content<TimestampedElement<I>, TimestampedElement<I>, R> {

    Queue<TimestampedElement<I>> content = new LinkedList<>();
    R emptyContent;
    long windowSize;
    Function<TimestampedElement<I>, R> f1;
    BiFunction<R, R, R> sumR;

    public LogicalSlidingContent(R emptyContent, long windowSize, Function<TimestampedElement<I>, R> f1, BiFunction<R, R, R> sumR){
        this.emptyContent = emptyContent;
        this.windowSize = windowSize;
        this.f1 = f1;
        this.sumR = sumR;
    }
    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(TimestampedElement<I> timestampedElement) {
        content.offer(timestampedElement);
        while(!content.isEmpty() && timestampedElement.getTimestamp() - windowSize >= content.peek().getTimestamp()){
            content.poll();
        }
    }

    @Override
    public R coalesce() {
        return content.stream().map(x->f1.apply(x)).reduce(emptyContent, (r1, r2) -> sumR.apply(r1, r2));
    }
}
