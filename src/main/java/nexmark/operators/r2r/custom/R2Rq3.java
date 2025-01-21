package nexmark.operators.r2r.custom;

import nexmark.customdatatypes.custom.*;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class R2Rq3 implements RelationToRelationOperator<List<Entity>> {

    /*
       SELECT person.name, person.city,
       person.state, open auction.id
       FROM open auction, person, item
       WHERE open auction.sellerId = person.id
       AND person.state = ‘OR’
       AND open auction.itemid = item.id
       AND item.categoryId = 10;

       */
    List<String> tvgNames;
    String resName;

    public R2Rq3(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public List<Entity> eval(List<List<Entity>> list) {
        List<Entity> people = list.get(0);
        List<Entity> auction = list.get(1);
        List<Entity> result = new ArrayList<>();
        people = people.stream().map(p->(PersonEvent)p).filter(p->p.state.equals("OR")
                ||p.state.equals("CA")
                ||p.state.equals("ID"))
                .collect(Collectors.toList());
        auction = auction.stream().map(a->(AuctionEvent)a).filter(a->a.category==10).collect(Collectors.toList());

        for(Entity p: people){
            PersonEvent person = (PersonEvent) p;
            for(Entity a: auction){
                AuctionEvent auct = (AuctionEvent) a;
                if(auct.seller == person.id){
                    result.add(new Q3Event(person.name, person.city, person.state, auct.id));
                }
            }
        }
        return result;
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
