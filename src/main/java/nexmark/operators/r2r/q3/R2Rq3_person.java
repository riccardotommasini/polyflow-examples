package nexmark.operators.r2r.q3;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.Table;

import java.util.List;

public class R2Rq3_person implements RelationToRelationOperator<Table> {

    List<String> tvgNames;
    String resName;

    public R2Rq3_person(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public Table eval(List<Table> list) {
        Table t = list.get(0);
        if(t.isEmpty())
            return t;

        return t.where(t.stringColumn("state").isIn(List.of("CA", "OR", "ID")));
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
