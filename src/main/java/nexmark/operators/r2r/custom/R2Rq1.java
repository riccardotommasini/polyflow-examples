package nexmark.operators.r2r.custom;

import nexmark.customdatatypes.BidEvent;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class R2Rq1 implements RelationToRelationOperator<List<Serializable>> {

    List<String> tvgNames;
    String resName;

    public R2Rq1(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public List<Serializable> eval(List<List<Serializable>> list) {
        List<Serializable> data = list.get(0);
        List<Serializable> res = new ArrayList<>();
        data.stream().map(d->(BidEvent)d).map(d->{
            BidEvent s = d.copy(d);
            s.price = (long) (s.price*0.85);
            return s;
        }).forEach(res::add);
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
