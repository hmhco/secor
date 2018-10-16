package com.pinterest.secor.common;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class SecorSchemaRegistryClient {

    private static final Logger LOG = LoggerFactory.getLogger(SecorSchemaRegistryClient.class);

    protected KafkaAvroDecoder decoder;
    private final static Map<String, Schema> schemas = new ConcurrentHashMap<>();
    protected SchemaRegistryClient schemaRegistryClient;

    public SecorSchemaRegistryClient(SecorConfig config) {
        try {
            Properties props = new Properties();
            props.put("schema.registry.url", config.getSchemaRegistryUrl());
            schemaRegistryClient = new CachedSchemaRegistryClient(config.getSchemaRegistryUrl(), 30);
            init(config);
        } catch (Exception e){
            LOG.error("Error initalizing schema registry", e);
            throw new RuntimeException(e);
        }
    }

    //Allows the SchemaRegistryClient to be mocked in unit tests
    protected void init(SecorConfig config) {
        decoder = new KafkaAvroDecoder(schemaRegistryClient);
    }

    private String getMagicByte(byte [] bytes){
        return bytes[0]+" "+bytes[1]+" "+bytes[2]+" "+bytes[3]+" "+bytes[4];
    }

    public GenericRecord decodeMessage(String topic, byte[] message) {
        GenericRecord record = (GenericRecord) decoder.fromBytes(message);
        Schema schema = record.getSchema();
        String magicByte = getMagicByte(message);
        System.out.println("decodedMessage topic["+topic+"] schema["+schema+"] magicByte["+ magicByte +"]");
        Schema schema1 = schemas.get(topic);
        if(schemas.containsKey(topic)){
            System.out.println("schemas already contains topic["+topic+"] schema["+schema1+"]");
        }
//        System.out.println("Adding schema["+schema1+"]  magicByte["+ magicByte +"] topic["+topic+"]");
//        schemas.put(topic, schema);
        return record;
    }

    public Schema getSchema(String topic) {
        Schema schema = schemas.get(topic);
        if (schema == null) {
            try {
                schema = lookupSchema(topic);
            } catch (IOException|RestClientException exc) {
                throw new IllegalStateException("Avro schema not found for topic " + topic);
            }
        }
        System.out.println("getSchema topic["+topic+"] schema["+schema+"]");
        return schema;
    }

    public Schema lookupSchema(String topic) throws IOException, RestClientException {
        String schema_string = schemaRegistryClient.getLatestSchemaMetadata(topic).getSchema();
        Schema schema = (new Schema.Parser()).parse(schema_string);
        System.out.println("lookupSchema topic["+topic+"] schema["+schema+"] schema_string["+schema_string+"]");
        schemas.put(topic, schema);
        return schema;
    }
}