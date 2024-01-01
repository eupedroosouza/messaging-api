package com.github.eupedroosouza.messaging.messenger;

import com.github.eupedroosouza.messaging.JedisMockServer;
import com.github.eupedroosouza.messaging.receiver.binary.ByteArrayMessageReceiver;
import com.github.eupedroosouza.messaging.sender.binary.ByteArrayMessageSender;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ByteArrayMessageSenderReceiverTest {

    public static final Logger LOGGER = Logger.getLogger(ByteArrayMessageSenderReceiverTest.class.getName());

    private int readMessages = 0;

    private ByteArrayMessageSender byteArrayMessageSender;
    private ByteArrayMessageReceiver byteArrayMessageReceiver;

    @BeforeAll
    void start() throws IOException {
        JedisMockServer jedisMockServer = JedisMockServer.getInstance();
        byteArrayMessageSender = new ByteArrayMessageSender(jedisMockServer.jedisExecutions, "bam");
        byteArrayMessageReceiver = new ByteArrayMessageReceiver(jedisMockServer.jedisExecutions, "bam") {
            @Override
            public void receive(byte[] message) {
                readMessages++;
                LOGGER.info(String.format("Received message %s", new String(message, StandardCharsets.UTF_8)));
            }
        };
        byteArrayMessageReceiver.start();
    }

    @Test
    @Order(1)
    void send() {
        byteArrayMessageSender.send("Hello world!".getBytes());
    }

    @Test
    @Order(2)
    void receive() {
        assertEquals(readMessages, 1);
    }

    @AfterAll
    void stop() {
        byteArrayMessageReceiver.shutdown();
    }

}
