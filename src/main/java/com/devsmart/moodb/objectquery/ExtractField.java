package com.devsmart.moodb.objectquery;


import com.devsmart.moodb.objects.DBElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

public class ExtractField implements ObjectOperation {

    Logger logger = LoggerFactory.getLogger(ExtractField.class);

    public final String memberName;

    public ExtractField(String memberName) {
        this.memberName = memberName;
    }

    @Override
    public DBElement eval(DBElement input) {
        if(!input.isObject()) {
            return null;
        } else {
            return input.getAsObject().get(memberName);
        }
    }

}
