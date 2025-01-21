package nexmark.content.custom;

import nexmark.customdatatypes.custom.BidEvent;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;

import nexmark.customdatatypes.custom.Entity;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvictContainerContent implements Content<Entity, Entity, List<Entity>> {

    private Map<Long, Content<Entity, Entity, List<Entity>>> keyedContent = new HashMap<>();
    ContentFactory<Entity, Entity, List<Entity>> internalContentFactory;

    public EvictContainerContent(ContentFactory<Entity, Entity, List<Entity>> internalContentFactory ){

        this.internalContentFactory = internalContentFactory;
    }

    @Override
    public int size() {
        return keyedContent.size();
    }

    @Override
    public void add(Entity i) {
        BidEvent event = (BidEvent)i;
        Long key = event.auction;
        keyedContent.computeIfAbsent(key, k->internalContentFactory.create());
        keyedContent.get(key).add(i);
    }

    @Override
    public List<Entity> coalesce() {
        return keyedContent.values().stream().map(c->c.coalesce()).reduce(new ArrayList<>(), (r1, r2)->{
            List<Entity> res = new ArrayList<>();
            res.addAll(r1);
            res.addAll(r2);
            return res;
        });
    }

    public void removeKey(Long key){
        keyedContent.remove(key);
    }
}