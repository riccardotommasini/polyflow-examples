package nexmark.operators.r2r.custom;

import nexmark.customdatatypes.custom.*;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class R2Rq4 implements RelationToRelationOperator<List<Entity>> {

    List<String> tvgNames;
    String resName;

    public R2Rq4(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public List<Entity> eval(List<List<Entity>> list) {
        List<Entity> auction = list.get(0);
        List<Entity> bid = list.get(1);
        List<Entity> result = new ArrayList<>();
        Map<Long, AuctionEvent> idToAuction = new HashMap<>();
        Map<Long, List<Long>> categories = new HashMap<>();

        if(auction.isEmpty())
            return auction;
        if(bid.isEmpty())
            return bid;

        auction.forEach(a->{
            AuctionEvent event = (AuctionEvent) a;
            idToAuction.put(event.id, event);
            categories.put(event.category, new ArrayList<>());

        });
        bid.forEach(b->{
            BidEvent event = (BidEvent) b;
            if(idToAuction.containsKey(event.auction)){
                categories.get(idToAuction.get(event.auction).category).add(event.price);
            }
        });
        categories.keySet().forEach(s->{
            double average;
            double sum = 0;
            for(Long v : categories.get(s)){
                sum+=v;
            }
            average = sum/categories.get(s).size();
            result.add(new Q4Event(average, s));
        });
        return result;
    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName;
    }
}
