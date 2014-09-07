package com.devsmart.moodb.query.parser;


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
    public Object eval(Object input) {
        Object retval = null;
        try {
            Field field = getField(input.getClass());
            field.setAccessible(true);
            retval = field.get(input);
        } catch (Exception e){
            logger.warn("cannot get field", e);
        }
        return retval;
    }

    private Field getField(Class<?> classType) {
        if(classType == null) {
            return null;
        }
        try {
            Field retval = classType.getDeclaredField(memberName);
            return retval;
        } catch (NoSuchFieldException e) {}

        return getField(classType.getSuperclass());
    }
}
