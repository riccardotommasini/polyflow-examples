package nexmark.operators.r2s;

import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;

import nexmark.customdatatypes.custom.Entity;

import java.util.List;
import java.util.stream.Stream;

public class R2SCustom implements RelationToStreamOperator<List<Entity>, Entity> {
    @Override
    public Stream<Entity> eval(List<Entity> sml, long ts) {
        return sml.stream();
    }

}
