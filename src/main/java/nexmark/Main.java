package nexmark;

import nexmark.queries.*;
import nexmark.utils.Query;

import java.util.ArrayList;
import java.util.List;

public class Main {


    public static void main(String[] args) {

        int iterations = 10;

        List<Query> queries = new ArrayList<>();

        queries.add(new Query1());
        queries.add(new Query2());
        queries.add(new Query3());
        queries.add(new Query4());
        queries.add(new Query5());
        queries.add(new Query6());
        queries.add(new Query7());
        queries.add(new Query8());


        for(Query q : queries){
            for(int i = 0; i<iterations; i++){

            }
        }

    }
}
