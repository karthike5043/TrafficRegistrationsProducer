package com.TrafficRegistrations.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.util.Iterator;
import java.util.Properties;

public class TrafficRegistrationPointsApp {
    static String topicName = "trafficRegistrationPoints";
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("parse.key","true");
        props.put("key.separator","id");

        KafkaProducer< String, String > sampleProducer = new KafkaProducer < String, String > (props);
        File file=new File("/Users/karthikeyan/Tolletaten/traffic-registrations.json");    //creates a new file instance
        FileReader fr=new FileReader(file);   //reads the file
        JSONParser jsonParser = new JSONParser();

        //Read JSON file
        JSONObject obj = (JSONObject) jsonParser.parse(fr);

        JSONObject trafficRegistrationPoints = (JSONObject) obj.get("data");
        JSONArray trafficRegistrationPoints2 = (JSONArray) trafficRegistrationPoints.get("trafficRegistrationPoints");

        Iterator<JSONObject> iterator = trafficRegistrationPoints2.iterator();

        while (iterator.hasNext()) {
            JSONObject currentVal = iterator.next();
            ProducerRecord< String, String > record
                    = new ProducerRecord < String, String >
                    (topicName,
                    (String) currentVal.get("id"), currentVal.toString());
            sampleProducer.send(record);
        }

        sampleProducer.close();
    }
}