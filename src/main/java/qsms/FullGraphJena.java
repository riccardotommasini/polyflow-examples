package qsms;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.ResultBinding;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.ArrayList;
import java.util.List;

public class FullGraphJena implements RelationToRelationOperator<QueryOrBindings> {
    private final Model graph;

    private List<String> tvgNames;

    private String resName;
    public FullGraphJena(Model graphMem, List<String> tvgNames, String resName) {
        this.graph = graphMem;
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public QueryOrBindings eval(List<QueryOrBindings> list) {
        List<Binding> bindings = new ArrayList<>();
        QueryOrBindings queryOrBindings = list.get(0);
        Query content = queryOrBindings.getContent();
        if(content!=null) {
            ResultSet resultSet = QueryExecutionFactory.create(content, graph).execSelect();
            while (resultSet.hasNext()) {
                bindings.add(((ResultBinding) resultSet.next()).getBinding());
            }
            queryOrBindings.setResult(bindings);
        }
        return queryOrBindings;
    }

    @Override
    public TimeVarying<QueryOrBindings> apply(TimeVarying<QueryOrBindings> node) {
        return RelationToRelationOperator.super.apply(node);
    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName ;
    }
}
