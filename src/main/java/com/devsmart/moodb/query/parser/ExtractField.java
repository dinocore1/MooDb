package com.devsmart.moodb.query.parser;


import java.lang.reflect.Field;

public class ExtractField implements ObjectOperation {

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
            e.printStackTrace();
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
