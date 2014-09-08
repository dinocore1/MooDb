package com.devsmart.moodb.objects;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.Iterator;

public class JsonElementDBWrapper {

    private static class DBJsonObject extends DBObject {

        private final JsonObject mJsonObj;

        public DBJsonObject(JsonObject jsonObj) {
            mJsonObj = jsonObj;
        }

        @Override
        public DBElement get(String fieldName) {
            return JsonElementDBWrapper.wrap(mJsonObj.get(fieldName));
        }

        @Override
        public DBCollection getAsCollection() {
            return null;
        }

        @Override
        public DBObject getAsObject() {
            return this;
        }

        @Override
        public DBPrimitive getAsPrimitive() {
            return null;
        }
    }

    private static class DBJsonArray extends DBCollection {

        private final JsonArray mArray;

        public DBJsonArray(JsonArray array) {
            mArray = array;
        }

        @Override
        public DBCollection getAsCollection() {
            return this;
        }

        @Override
        public DBObject getAsObject() {
            return null;
        }

        @Override
        public DBPrimitive getAsPrimitive() {
            return null;
        }

        @Override
        public Iterator<DBElement> iterator() {
            return new Iterator<DBElement>() {

                int i = -1;

                @Override
                public boolean hasNext() {
                    if(mArray.size() == 0) {
                        return false;
                    }
                    return i < mArray.size()-1;
                }

                @Override
                public DBElement next() {
                    return JsonElementDBWrapper.wrap(mArray.get(++i));
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    private static class DBJsonPrimitive extends DBPrimitive {

        private final JsonPrimitive mPrimitive;

        public DBJsonPrimitive(JsonPrimitive primitive) {
            mPrimitive = primitive;
        }

        @Override
        public boolean isString() {
            return mPrimitive.isString();
        }

        @Override
        public String getAsString() {
            return mPrimitive.getAsString();
        }

        @Override
        public boolean isNumber() {
            return mPrimitive.isNumber();
        }

        @Override
        public double getAsDouble() {
            return mPrimitive.getAsDouble();
        }

        @Override
        public boolean isBoolean() {
            return mPrimitive.isBoolean();
        }

        @Override
        public boolean getAsBoolean() {
            return mPrimitive.getAsBoolean();
        }

        @Override
        public DBCollection getAsCollection() {
            return null;
        }

        @Override
        public DBObject getAsObject() {
            return null;
        }

        @Override
        public DBPrimitive getAsPrimitive() {
            return this;
        }
    }

    public static DBElement wrap(JsonElement element) {
        if(element == null) {
            return null;
        }

        if(element.isJsonArray()) {
            return new DBJsonArray(element.getAsJsonArray());
        } else if(element.isJsonObject()) {
            return new DBJsonObject(element.getAsJsonObject());
        } else if(element.isJsonPrimitive()) {
            return new DBJsonPrimitive(element.getAsJsonPrimitive());
        }
        return null;
    }
}
