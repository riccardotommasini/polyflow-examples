package nexmark.operators.r2s;

import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class R2SCustom implements RelationToStreamOperator<List<Serializable>, Serializable> {
    @Override
    public Stream<Serializable> eval(List<Serializable> sml, long ts) {
        return sml.stream();
    }

}
