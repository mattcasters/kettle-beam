package org.kettle.beam.core.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;


public class Json {

    //region Attributes

    private static Json instance;
    private static Object lockInstance = new Object();

    //endregion

    //region Construtors

    private Json(){}

    //endregion

    //region Methods

    public static Json getInstance(){
        if(instance == null){
            synchronized (lockInstance){
                if(instance == null){instance = new Json();}
            }
        }
        return instance;
    }

    public String serialize(Object entity) throws Exception {
        if(entity == null){return null;}
        ObjectMapper mapper = new ObjectMapper();
        String jsonResult = mapper.writeValueAsString(entity);
        return jsonResult;
    }

    public <T> T deserialize(Class<T> type, String json) throws Exception{
        if(type == null || Strings.isNullOrEmpty(json)){return null;}
        ObjectMapper mapper = new ObjectMapper();
        T entity = (T)mapper.readValue(json, type);
        return entity;
    }

    public Map<String, Object> deserialize(String json) throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};
        Map<String, Object> map = mapper.readValue(json, typeRef);
        return map;
    }

    //endregion


}
