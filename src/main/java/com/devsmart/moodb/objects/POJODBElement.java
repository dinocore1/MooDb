package com.devsmart.moodb.objects;


import java.util.Collection;

public class POJODBElement  {


    public static DBElement wrap(Object obj) {
        if(obj.getClass().isArray()) {
            return new POJOArray(obj);
        } else if(obj instanceof Collection){
            return new POJOCollection((Collection) obj);
        } else if(POJOPrimitive.isPrimitiveOrString(obj)) {
            return new POJOPrimitive(obj);
        } else {
            return new POJOObject(obj);
        }
    }


}
