package nexmark.operators.r2r;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;

import java.util.List;

public class R2Rq1 implements RelationToRelationOperator<Table> {

    List<String> tvgNames;
    String resName;

    public R2Rq1(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public Table eval(List<Table> list) {
        Table t = list.get(0);
        if(t.isEmpty())
            return t;
        t.addColumns(t.column("price").mapInto(v->(double)((long)v*0.908), DoubleColumn.create("priceDollar",t.column(0).size())));
        return t;
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
