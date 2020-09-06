package org.example.rkredux.avro.generic;

import com.example.Customer;
import com.example.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.File;
import java.io.IOException;

public class SchemaEvolution{

    public static void main(String[] args) {
//        writing the record
        Customer.Builder builder = Customer.newBuilder();
        builder.setFirstName("Rahul");
        builder.setLastName("Kumar");
        builder.setWeight(78.6f);
        builder.setHeight(178.9f);
        builder.setAge(35);
        builder.setAutomatedEmail(false);
        final Customer customerV1record = builder.build();

        SpecificDatumWriter specificCustomerV1DatumWriter = new SpecificDatumWriter(Customer.class);
        DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(specificCustomerV1DatumWriter);
        try {
            dataFileWriter.create(customerV1record.getSchema(), new File("customerV1-specific.avro"));
            dataFileWriter.append(customerV1record);
            System.out.println(String.format("Successfully written avro record with schema %s to the file", customerV1record.getSchema().getName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
//        reading the records
        final File file = new File("customerV1-specific.avro");
        SpecificDatumReader<CustomerV2> recordSpecificDatumReader = new SpecificDatumReader<>(CustomerV2.class);
        try {
            DataFileReader<CustomerV2> dataFileReader;
            dataFileReader = new DataFileReader<>(file, recordSpecificDatumReader);
            while (dataFileReader.hasNext()){
                System.out.println("Starting reading file");
                CustomerV2 readCustomer = dataFileReader.next();
                System.out.println(readCustomer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
