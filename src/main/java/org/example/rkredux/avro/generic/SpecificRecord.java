package org.example.rkredux.avro.generic;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import java.io.File;
import java.io.IOException;

public class SpecificRecord {

    public static void main(String[] args) {
        Customer.Builder builder = Customer.newBuilder();
        builder.setFirstName("Rahul");
        builder.setLastName("Kumar");
        builder.setAge(34);
        builder.setHeight(178f);
        builder.setWeight(87f);
        builder.setAutomatedEmail(true);
        Customer customer = builder.build();

//        write to a file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try {
            DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("Successfully wrote avro record to the file");
        } catch (IOException e) {
            System.out.println("Could not write file");
            e.printStackTrace();
        }

//        read from a file
        System.out.println("Starting reading file");
        final File file = new File("customer-specific.avro");
        SpecificDatumReader<Customer> recordSpecificDatumReader = new SpecificDatumReader<>(Customer.class);
        try {
            DataFileReader<Customer> dataFileReader;
            dataFileReader = new DataFileReader<>(file, recordSpecificDatumReader);
            while (dataFileReader.hasNext()){
                System.out.println("Starting reading file");
                Customer readCustomer = dataFileReader.next();
                System.out.println(readCustomer.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
