package nexmark.content.custom;

import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.EmptyContent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvictContainerFactory  implements ContentFactory<Serializable, Serializable, List<Serializable>> {

    private EvictContainerContent singleton;

    private ContentFactory<Serializable, Serializable, List<Serializable>> internalContentFactory;

    public EvictContainerFactory(ContentFactory<Serializable, Serializable, List<Serializable>> internalContentFactory ){
        this.internalContentFactory = internalContentFactory;
    }

    @Override
    public Content<Serializable, Serializable, List<Serializable>> createEmpty() {
        return new EmptyContent<>(new ArrayList<>());
    }

    @Override
    public Content<Serializable, Serializable, List<Serializable>> create() {
        if(singleton == null){
            singleton = new EvictContainerContent(internalContentFactory);
        }
        return singleton;
    }
}