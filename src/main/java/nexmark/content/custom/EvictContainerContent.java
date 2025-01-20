package nexmark.content.custom;

import nexmark.customdatatypes.BidEvent;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvictContainerContent implements Content<Serializable, Serializable, List<Serializable>> {

    private Map<Long, Content<Serializable, Serializable, List<Serializable>>> keyedContent = new HashMap<>();
    ContentFactory<Serializable, Serializable, List<Serializable>> internalContentFactory;

    public EvictContainerContent(ContentFactory<Serializable, Serializable, List<Serializable>> internalContentFactory ){

        this.internalContentFactory = internalContentFactory;
    }

    @Override
    public int size() {
        return keyedContent.size();
    }

    @Override
    public void add(Serializable i) {
        BidEvent event = (BidEvent)i;
        Long key = event.auction;
        keyedContent.computeIfAbsent(key, k->internalContentFactory.create());
        keyedContent.get(key).add(i);
    }

    @Override
    public List<Serializable> coalesce() {
        return keyedContent.values().stream().map(c->c.coalesce()).reduce(new ArrayList<>(), (r1, r2)->{
            List<Serializable> res = new ArrayList<>();
            res.addAll(r1);
            res.addAll(r2);
            return res;
        });
    }

    public void removeKey(Long key){
        keyedContent.remove(key);
    }
}