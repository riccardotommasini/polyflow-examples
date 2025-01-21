package nexmark.content.custom;

import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;

import nexmark.customdatatypes.custom.Entity;
import java.util.ArrayList;
import java.util.List;

public class EvictContainerFactory  implements ContentFactory<Entity, Entity, List<Entity>> {

    private EvictContainerContent singleton;

    private ContentFactory<Entity, Entity, List<Entity>> internalContentFactory;

    public EvictContainerFactory(ContentFactory<Entity, Entity, List<Entity>> internalContentFactory ){
        this.internalContentFactory = internalContentFactory;
    }

    @Override
    public Content<Entity, Entity, List<Entity>> createEmpty() {
        return new EmptyContent<>(new ArrayList<>());
    }

    @Override
    public Content<Entity, Entity, List<Entity>> create() {
        if(singleton == null){
            singleton = new EvictContainerContent(internalContentFactory);
        }
        return singleton;
    }
}