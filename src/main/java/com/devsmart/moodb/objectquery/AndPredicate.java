package com.devsmart.moodb.objectquery;


import com.devsmart.moodb.objects.DBElement;

public class AndPredicate implements Predicate {

    final Predicate mP1;
    final Predicate mP2;

    public AndPredicate(Predicate p1, Predicate p2) {
        mP1 = p1;
        mP2 = p2;
    }

    @Override
    public boolean matches(DBElement input) {
        return mP1.matches(input) && mP2.matches(input);
    }
}
