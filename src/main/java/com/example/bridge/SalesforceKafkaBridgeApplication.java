package com.example.bridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SalesforceKafkaBridgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(SalesforceKafkaBridgeApplication.class, args);
    }
}
