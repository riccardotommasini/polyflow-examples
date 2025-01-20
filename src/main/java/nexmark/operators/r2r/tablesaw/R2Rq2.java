package nexmark.operators.r2r.tablesaw;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;

import java.util.List;

public class R2Rq2 implements RelationToRelationOperator<Table> {

    List<String> tvgNames;
    String resName;

    public R2Rq2(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public Table eval(List<Table> list) {
        Table t = list.get(0);
        if(t.isEmpty())
            return t;
        return t.where(t.longColumn("auction").isIn(List.of(1007.0, 1020.0, 2001.0, 2019.0, 1087.0))).selectColumns("auction", "price");
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
