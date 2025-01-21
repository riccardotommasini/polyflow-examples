package nexmark.content.custom;

import nexmark.customdatatypes.custom.Entity;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;

import java.util.ArrayList;
import java.util.List;

public class Q8SlidingFactory  implements ContentFactory<Entity, Entity, List<Entity>> {

    int windowSize;

    public Q8SlidingFactory(int windowSize){
        this.windowSize = windowSize;
    }
    @Override
    public Content<Entity, Entity, List<Entity>> createEmpty() {
        return new EmptyContent<>(new ArrayList<>());
    }

    @Override
    public Content<Entity, Entity, List<Entity>> create() {
        return new Q8SlidingContent(windowSize);
    }
}