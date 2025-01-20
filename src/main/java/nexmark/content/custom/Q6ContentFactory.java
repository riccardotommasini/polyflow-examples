package nexmark.content.custom;

import nexmark.content.custom.Q6Content;
import nexmark.customdatatypes.TimestampedElement;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;
import tech.tablesaw.api.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Q6ContentFactory implements ContentFactory<Serializable, Serializable, List<Serializable>> {

    int windowSize;
    EvictContainerContent bidContent;

    public Q6ContentFactory(int windowSize, EvictContainerContent bidContent){
        this.windowSize = windowSize;
        this.bidContent = bidContent;
    }
    @Override
    public Content<Serializable, Serializable, List<Serializable>> createEmpty() {
        return new EmptyContent<>(new ArrayList<>());
    }

    @Override
    public Content<Serializable, Serializable, List<Serializable>> create() {
        return new Q6Content(windowSize, bidContent);
    }
}


