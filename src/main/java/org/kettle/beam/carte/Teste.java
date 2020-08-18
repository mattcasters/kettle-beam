package org.kettle.beam.carte;


import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author 32214_thiago
 */
public class Teste {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        try {
            String event = "{\"body\":{\"horario_minuto_tolerancia_pos_0\":480,\"escalaData\":\"2020-07-23T21:00:00.000-03:00\",\"horario_minuto_tolerancia_pos_1\":680,\"horario_minuto_tolerancia_pos_2\":710,\"horario_minuto_tolerancia_pos_3\":870,\"colaboradorMatricula\":\"23\",\"registros\":4,\"horario_tipo_1\":4,\"horario_minuto_tolerancia_pre_2\":710,\"horario_minuto_3\":870,\"horario_tipo_0\":1,\"horario_minuto_tolerancia_pre_1\":680,\"horario_minuto_2\":710,\"horario_minuto_tolerancia_pre_0\":480,\"horario_minuto_1\":680,\"horario_minuto_0\":480,\"horario_tipo_3\":1,\"horario_tipo_2\":4,\"horario_minuto_tolerancia_pre_3\":870,\"horario_sequencia_3\":4,\"escalaDescricao\":\"OP 08:00-11:20-11:50-14:30\",\"horario_sequencia_2\":3,\"horario_sequencia_1\":2,\"horario_sequencia_0\":1},\"header\":{\"monitorSource\":\"Senior Escala\",\"colectorHostname\":\"fe80:0:0:0:447d:57ff:fe1f:45c5%vethae5377c;fe80:0:0:0:42:f3ff:fee3:f04c%docker0;172.17.0.1;fe80:0:0:0:4001:aff:feab:25%eth0;monitoramento-senior-escala-preemptive.c.clk-big-data.internal;localhost\",\"subject\":\"Agent Status\",\"monitorPlatform\":\"Senior\",\"monitorComponentName\":\"Senior Escala Monitor v1.0\",\"timeColector\":\"2020-07-24T13:26:11.843-03:00\",\"colectorIp\":\"fe80:0:0:0:447d:57ff:fe1f:45c5%vethae5377c;fe80:0:0:0:42:f3ff:fee3:f04c%docker0;172.17.0.1;fe80:0:0:0:4001:aff:feab:25%eth0;10.171.0.37\"}}";
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.USE_LONG_FOR_INTS, true);

            // convert JSON string to Map
            java.util.Map<String, Object> eventMap = (java.util.Map) mapper.readValue(event, java.util.Map.class);
            java.util.Map<String, Object> body = (java.util.Map) eventMap.get("body");
            
            Datastore datastore = DatastoreOptions.getDefaultInstance().getService();            
            
            Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind("dev")
                .build();

            QueryResults<Entity> results = datastore.run(query);
//
//    //        List<Entity> allData = new ArrayList<>();
//
//            while (results.hasNext()) {
//
//                Entity currentEntity = results.next();
//                System.out.println(currentEntity.getString("Descrição"));
//    //            allData.add(currentEntity);
//            }
            
        } catch (Exception e){
            
            e.printStackTrace();
        }
    }
    
}
