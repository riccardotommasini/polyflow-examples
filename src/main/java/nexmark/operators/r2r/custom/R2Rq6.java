package nexmark.operators.r2r.custom;

import nexmark.customdatatypes.AuctionEvent;
import nexmark.customdatatypes.BidEvent;
import nexmark.customdatatypes.Q6Event;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class R2Rq6 implements RelationToRelationOperator<List<Serializable>> {

    List<String> tvgNames;
    String resName;

    public R2Rq6(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public List<Serializable> eval(List<List<Serializable>> list) {
        List<Serializable> auction = list.get(0);
        List<Serializable> bid = list.get(1);
        List<Serializable> result = new ArrayList<>();
        Map<Long, AuctionEvent> idToAuction = new HashMap<>();
        Map<Long, List<Long>> sellers = new HashMap<>();

        if(auction.isEmpty())
            return auction;
        if(bid.isEmpty())
            return bid;

        auction.forEach(a->{
            AuctionEvent event = (AuctionEvent) a;
            idToAuction.put(event.id, event);
            sellers.put(event.seller, new ArrayList<>());

        });
        bid.forEach(b->{
            BidEvent event = (BidEvent) b;
            if(idToAuction.containsKey(event.auction)){
                sellers.get(idToAuction.get(event.auction).seller).add(event.price);
            }
        });
       sellers.keySet().forEach(s->{
               double average;
               double sum = 0;
               for(Long v : sellers.get(s)){
                   sum+=v;
               }
               average = sum/sellers.get(s).size();
               result.add(new Q6Event(average, s));
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
