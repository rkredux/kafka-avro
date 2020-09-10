package org.example.rkredux.avro.generic;

import com.example.Customer;
import com.example.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import java.io.File;
import java.io.IOException;

public class SchemaEvolution{

    public static void main(String[] args) {
        //record serialization
        Customer.Builder builder = Customer.newBuilder();
        builder.setFirstName("Rahul");
        builder.setLastName("Kumar");
        builder.setWeight(78.6f);
        builder.setHeight(178.9f);
        builder.setAge(35);
        builder.setAutomatedEmail(false);
        builder.setPhoneNumber("999-999-9999");
        final Customer customerV1record = builder.build();

        DatumWriter<Customer> customerV1DatumWriter = new SpecificDatumWriter<>(Customer.class);
        DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(customerV1DatumWriter);
        try {
            dataFileWriter.create(customerV1record.getSchema(), new File("customerV1-specific.avro"));
            dataFileWriter.append(customerV1record);
            dataFileWriter.close();
            System.out.println(String.format("Successfully written avro record with schema %s to the file", customerV1record.getSchema().getName()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //deserialization
        final File file = new File("customerV1-specific.avro");
        DatumReader<CustomerV2> customerV2DatumReader = new SpecificDatumReader<>(CustomerV2.class);
        try {
            DataFileReader<CustomerV2> dataFileReader;
            dataFileReader = new DataFileReader<>(file, customerV2DatumReader);
            CustomerV2 customerV2 = null;
            while (dataFileReader.hasNext()){
                System.out.println("About to read");
                customerV2 = dataFileReader.next(customerV2);
                System.out.println(customerV2);
                System.out.println(customerV2.getFirstName());
                System.out.println(String.format("The customer's first name is %s and last name is %s", customerV2.getFirstName(), customerV2.getLastName()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
