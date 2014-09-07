package com.devsmart.moodb.query.parser;


import java.lang.reflect.Field;

public class ExtractField {

    public final String memberName;

    public ExtractField(String memberName) {
        this.memberName = memberName;
    }

    public Object eval(Object input) {
        Object retval = null;
        try {
            Field field = input.getClass().getField(memberName);
            retval = field.get(input);
        } catch (Exception e){}
        return retval;
    }
}
