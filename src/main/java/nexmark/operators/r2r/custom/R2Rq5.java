package nexmark.operators.r2r.custom;

import nexmark.customdatatypes.custom.BidEvent;
import nexmark.customdatatypes.custom.Entity;
import nexmark.customdatatypes.custom.Q5Event;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Table;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class R2Rq5 implements RelationToRelationOperator<List<Entity>> {

    List<String> tvgNames;
    String resName;

    public R2Rq5(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public List<Entity> eval(List<List<Entity>> list) {
        List<Entity> t = list.get(0);
        Map<Long, Long> counter = new HashMap<>();

        for(int i =0; i<t.size(); i++){
            Long key = ((BidEvent) t.get(i)).auction;
            if(!counter.containsKey(key)){
                counter.put(key, 1L);
            }
            else{
                counter.put(key, counter.get(key)+1);
            }
        }
        long max = 0;
        long maxKey = 0;
        for(Long k : counter.keySet()){
            if(counter.get(k)>max) {
                max = counter.get(k);
                maxKey = k;
            }
        }
        return List.of(new Q5Event(maxKey, max));

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
