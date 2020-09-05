package org.example.rkredux.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecord {

    public static void main(String[] args) {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "    \"type\": \"record\",\n" +
                "    \"namespace\": \"com.example\",\n" +
                "    \"name\": \"Customer\",\n" +
                "    \"doc\": \"Avro Schema for our customers\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"first_name\", \"type\": \"string\", \"doc\": \"first name of the customer\"},\n" +
                "        {\"name\": \"last_name\", \"type\": \"string\", \"doc\": \"last name of the customer\"},\n" +
                "        {\"name\": \"age\", \"type\": \"int\", \"doc\": \"age of the customer\"},\n" +
                "        {\"name\": \"height\", \"type\": \"float\", \"doc\": \"height in cms\"},\n" +
                "        {\"name\": \"weight\", \"type\": \"float\", \"doc\": \"weight in kilograms\"},\n" +
                "        {\"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"true if the user wants marketing emails\"}\n" +
                "    ]\n" +
                "}");
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "John");
        customerBuilder.set("last_name", "Oliver");
        customerBuilder.set("age", 70);
        customerBuilder.set("height", 178.9f);
        customerBuilder.set("weight", 78.8f);
        customerBuilder.set("automated_email", false);
        GenericData.Record customer = customerBuilder.build();
        System.out.println(customer);

//        write a generic record to the file
        final DatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(schema);
        try {
            DataFileWriter<GenericData.Record> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, new File("customer-generic.avro"));
            dataFileWriter.append(customer);
        } catch (IOException e) {
            System.out.println("Could not write file");
            e.printStackTrace();
        }

//        read a generic record from a file
        final File file = new File("customer-generic.avro");
        GenericDatumReader<GenericData.Record> recordGenericDatumReader = new GenericDatumReader<>();
        GenericData.Record customerRead;
        try {
            DataFileReader<GenericData.Record> dataFileReader = new DataFileReader<>(file, recordGenericDatumReader);
            customerRead = dataFileReader.next();
            System.out.println("Successfully read customer");
            System.out.println(customerRead.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
