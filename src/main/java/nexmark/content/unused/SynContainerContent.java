package nexmark.content.unused;

import nexmark.customdatatypes.tablesaw.TestTimestampedRow;
import nexmark.customdatatypes.tablesaw.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import tech.tablesaw.api.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SynContainerContent implements Content<TimestampedElement<Table>, TimestampedElement<Table>, List<Long>> {

    private Function<TimestampedElement<Table>, Long> keyFromI;
    private Map<Long, Content<TimestampedElement<Table>, TimestampedElement<Table>, Long>> keyedContent = new HashMap<>();
    ContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Long> internalContentFactory;

    public SynContainerContent(Function<TimestampedElement<Table>, Long> keyFromI ,
                               ContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Long> internalContentFactory ){
        this.keyFromI = keyFromI;
        this.internalContentFactory = internalContentFactory;
    }

    @Override
    public int size() {
        return keyedContent.size();
    }

    @Override
    public void add(TimestampedElement<Table> i) {
        Long key = keyFromI.apply(i);
        keyedContent.computeIfAbsent(key, k->internalContentFactory.create());
        keyedContent.get(key).add(i);
        for(Long k : keyedContent.keySet()){
            if(!k.equals(key)){ //For all the other keys, add a "fake" element to make the windows advance
                keyedContent.get(k).add(new TestTimestampedRow(null, i.getTimestamp()));
            }
        }
    }

    @Override
    public List<Long> coalesce() {
        Long maxKey = 0L;
        Long maxVal = 0L;
        for(Long key : keyedContent.keySet()){
            if(keyedContent.get(key).coalesce() > maxVal)
                maxKey = key;
        }
        return List.of(maxKey);
    }
}
