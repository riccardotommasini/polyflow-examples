package nexmark.operators.r2r.custom;

import nexmark.customdatatypes.custom.BidEvent;
import nexmark.customdatatypes.custom.Entity;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.Table;

import java.util.List;
import java.util.stream.Collectors;

public class R2Rq2 implements RelationToRelationOperator<List<Entity>> {

    List<String> tvgNames;
    String resName;

    public R2Rq2(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public List<Entity> eval(List<List<Entity>> list) {
        List<Entity> t = list.get(0);
       return t.stream().map(event->(BidEvent)event).filter(event-> event.auction==1007 || event.auction == 1020 || event.auction == 2001 || event.auction == 2019
               || event.auction == 1087).collect(Collectors.toList());
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
