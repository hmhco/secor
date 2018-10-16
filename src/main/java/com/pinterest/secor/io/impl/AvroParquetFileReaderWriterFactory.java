package com.pinterest.secor.io.impl;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.SecorSchemaRegistryClient;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.AvroSchemaUtil;
import com.pinterest.secor.util.ParquetUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class AvroParquetFileReaderWriterFactory implements FileReaderWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroParquetFileReaderWriterFactory.class);
    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean validating;
    protected final String schemaSubjectSuffix;
    protected final String schemaSubjectOverrideGlobal;
    protected final Map<String, String> schemaSubjectOverrideTopics;
    protected SecorSchemaRegistryClient schemaRegistryClient;

    public AvroParquetFileReaderWriterFactory(SecorConfig config) {
        blockSize = ParquetUtil.getParquetBlockSize(config);
        pageSize = ParquetUtil.getParquetPageSize(config);
        enableDictionary = ParquetUtil.getParquetEnableDictionary(config);
        validating = ParquetUtil.getParquetValidation(config);
        schemaSubjectSuffix = AvroSchemaUtil.getAvroSubjectSuffix(config);
        schemaSubjectOverrideGlobal = AvroSchemaUtil.getAvroSubjectOverrideGlobal(config);
        schemaSubjectOverrideTopics = AvroSchemaUtil.getAvroSubjectOverrideTopics(config);
        schemaRegistryClient = new SecorSchemaRegistryClient(config);
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroParquetFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroParquetFileWriter(logFilePath, codec);
    }

    protected static byte[] serializeAvroRecord(SpecificDatumWriter<GenericRecord> writer, GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        ByteBuffer serialized = ByteBuffer.allocate(out.toByteArray().length);
        serialized.put(out.toByteArray());
        return serialized.array();
    }

    protected static GenericRecord deserializeAvroRecord(SpecificDatumReader<GenericRecord> reader, byte[] value) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
        return reader.read(null, decoder);
    }


    private String getMagicByte(byte [] bytes){
        return bytes[0]+" "+bytes[1]+" "+bytes[2]+" "+bytes[3]+" "+bytes[4];
    }

    private String getPayload(byte [] bytes){
        return new String(bytes, StandardCharsets.UTF_8);
    }

    protected GenericRecord decodeMessage(byte[] value, String topic, SpecificDatumReader<GenericRecord> reader) throws IOException {
        // Avro schema registry header format is a "Magic Byte" that equals 0 followed by a 4-byte int
        // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format

        if (value.length > 5 && value[0] == 0) {
            System.out.println("AP decodeMessage topic["+topic+"] calling decodeMessage getMagicByte[" + getMagicByte(value) + "] payload[" + getPayload(value)+"]");
            return schemaRegistryClient.decodeMessage(topic, value);
        } else {
            System.out.println("AP decodeMessage topic["+topic+"] calling deserializeAvroRecord payload[" + getPayload(value)+"]");
            return deserializeAvroRecord(reader, value);
        }
    }

    protected String getSchemaSubjectOverride(String topic) {
        String subjectOverride = schemaSubjectOverrideTopics.get(topic);
        return (null != subjectOverride) ? subjectOverride : schemaSubjectOverrideGlobal;
    }

    protected Schema getSchema(String topic) {
        String subjectOverride = getSchemaSubjectOverride(topic);
        if (!subjectOverride.isEmpty()) {
            topic = subjectOverride;
        } else if (!schemaSubjectSuffix.isEmpty()) {
            topic += schemaSubjectSuffix;
        }
        return schemaRegistryClient.getSchema(topic);
    }

    protected class AvroParquetFileReader implements FileReader {

        private ParquetReader<GenericRecord> reader;
        private SpecificDatumWriter<GenericRecord> writer;
        private long offset;

        public AvroParquetFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            String topic = logFilePath.getTopic();
            Schema schema = getSchema(topic);
            reader = AvroParquetReader.<GenericRecord>builder(path).build();
            writer = new SpecificDatumWriter(schema);
            offset = logFilePath.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            GenericRecord record = reader.read();
            if (record != null) {
                return new KeyValue(offset++, serializeAvroRecord(writer, record));
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

    }

    protected class AvroParquetFileWriter implements FileWriter {

        private ParquetWriter writer;
        private String topic;
        private SpecificDatumReader<GenericRecord> datumReader;
        private Schema schema;
        private Path path;
        private CompressionCodecName codecName;

        public AvroParquetFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            path = new Path(logFilePath.getLogFilePath());
            LOG.debug("Creating Brand new Writer for path {}", path);
            codecName = CompressionCodecName
                    .fromCompressionCodec(codec != null ? codec.getClass() : null);
            topic = logFilePath.getTopic();
            schema = getSchema(topic);
            datumReader = new SpecificDatumReader<>(schema);

            // Not setting blockSize, pageSize, enableDictionary, and validating
            System.out.println("AvroParquetFileWriter schema["+schema+"]");
//            writer = AvroParquetWriter.builder(path)
//                    .withSchema(schema)
//                    .withCompressionCodec(codecName)
//                    .build();
        }

        @Override
        public long getLength() throws IOException {
            return writer.getDataSize();
        }



        @Override
        public void write(KeyValue keyValue) throws IOException {
            byte[] value = keyValue.getValue();
            GenericRecord record = decodeMessage(value, topic, datumReader);
            System.out.println("AP decodeMessage topic["+topic+"] record["+record+"] payload["+getPayload(value)+"]");

            LOG.trace("Writing record {}", record);
            if (record != null){
                try {
                    Schema schema = record.getSchema();
                    writer = AvroParquetWriter.builder(path)
                            .withSchema(schema)
                            .withCompressionCodec(codecName)
                            .build();
                    System.out.println("AP write["+writer+"] schema["+schema+"] topic["+topic+"] path["+path+"] record["+record+"] payload["+getPayload(value)+"]");
                    writer.write(record);
                } catch (ArrayIndexOutOfBoundsException e) {
                    LOG.warn("Skipping this record of missing timestamp field {}", record);
                }
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
