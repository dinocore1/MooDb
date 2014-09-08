package com.devsmart.moodb.objectquery;

import com.devsmart.moodb.objects.DBElement;

public class OrPredicate implements Predicate {
    private final Predicate mP1;
    private final Predicate mP2;

    public OrPredicate(Predicate p1, Predicate p2) {
        mP1 = p1;
        mP2 = p2;
    }

    @Override
    public boolean matches(DBElement input) {
        return mP1.matches(input) || mP2.matches(input);
    }
}
