package nexmark.operators.r2r.custom;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;

import nexmark.customdatatypes.custom.Entity;
import java.util.List;

public class R2Rq7 implements RelationToRelationOperator<List<Entity>> {
    List<String> tvgNames;
    String resName;

    public R2Rq7(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }
    @Override
    public List<Entity> eval(List<List<Entity>> list) {
        return list.get(0);
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
