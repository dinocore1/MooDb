package com.devsmart.moodb.objectquery;


import com.devsmart.moodb.objects.DBElement;

public interface Predicate {

    boolean matches(DBElement input);
}
