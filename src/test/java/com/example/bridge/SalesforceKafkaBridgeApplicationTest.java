package com.example.bridge;

import org.junit.jupiter.api.Test;

class SalesforceKafkaBridgeApplicationTest {

    @Test
    void mainClassExists() {
        // Verify the application class can be instantiated
        SalesforceKafkaBridgeApplication app = new SalesforceKafkaBridgeApplication();
        org.junit.jupiter.api.Assertions.assertNotNull(app);
    }
}
