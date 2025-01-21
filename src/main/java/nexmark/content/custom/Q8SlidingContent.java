package nexmark.content.custom;

import nexmark.customdatatypes.custom.BidEvent;
import nexmark.customdatatypes.custom.Entity;
import nexmark.customdatatypes.custom.PersonEvent;
import org.streamreasoning.polyflow.api.secret.content.Content;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Q8SlidingContent implements Content<Entity, Entity, List<Entity>> {

    int windowSize;
    Queue<Entity> content = new LinkedList<>();


    public Q8SlidingContent (int windowSize){
        this.windowSize = windowSize;
    }
    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(Entity entity) {
        content.offer(entity);
        PersonEvent bid = (PersonEvent) entity;
        while(!content.isEmpty()&& bid.timestamp-windowSize>= ((PersonEvent)content.peek()).timestamp){
            content.poll();
        }
    }

    @Override
    public List<Entity> coalesce() {
        return new ArrayList<>(content);
    }
}