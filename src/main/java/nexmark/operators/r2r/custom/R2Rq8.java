package nexmark.operators.r2r.custom;

import nexmark.customdatatypes.custom.AuctionEvent;
import nexmark.customdatatypes.custom.Entity;
import nexmark.customdatatypes.custom.PersonEvent;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class R2Rq8 implements RelationToRelationOperator<List<Entity>> {

    List<String> tvgNames;
    String resName;

    public R2Rq8(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public List<Entity> eval(List<List<Entity>> list) {
        List<Entity> people = list.get(0);
        List<Entity> auction = list.get(1);
        List<Entity> res = new ArrayList<>();
        Set<Long> isPresent = new HashSet<>();
        for(Entity a : auction){
            AuctionEvent auct = (AuctionEvent) a;
            isPresent.add(auct.seller);
        }
        for(Entity p : people){
            PersonEvent person = (PersonEvent) p;
            if(isPresent.contains(person.id))
                res.add(p);
        }
        return res;


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
