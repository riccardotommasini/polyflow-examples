package qsms;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class QueryOrBindings implements Iterable<Binding> {

    private List<Op> content;
    private List<Binding> result;

    public QueryOrBindings(Op content) {
        this.content = new ArrayList<>();
        this.content.add(content);
        this.result = new ArrayList();
    }

    public QueryOrBindings(QueryOrBindings q1, QueryOrBindings q2) {
        this.content = new ArrayList<>();
        this.content.addAll(q1.content);
        this.content.addAll(q2.content);
        this.result = new ArrayList();
        this.result.addAll(q1.result);
        this.result.addAll(q2.result);
    }


    public QueryOrBindings() {
        this.result = new ArrayList();
        this.content = new ArrayList();
    }

    public Iterator<Binding> iterator() {
        return this.result.iterator();
    }

    public void forEach(Consumer<? super Binding> action) {
        this.result.forEach(action);
    }

    public Query getContent() {
        if ((content != null && !content.isEmpty())) {
            Op reduce = this.content.stream().skip(1).reduce(this.content.get(0), OpUnion::create);
            System.out.println(reduce);
            Op optimize = Algebra.optimize(reduce);
            System.out.println(optimize);
            return OpAsQuery.asQuery(optimize);
        }
        return null;
    }

    public List<Binding> getResult() {
        return this.result;
    }

    public void setContent(Op content) {
        this.content = List.of(content);
    }

    public void setResult(List<Binding> res) {
        this.result = res;
    }
}
