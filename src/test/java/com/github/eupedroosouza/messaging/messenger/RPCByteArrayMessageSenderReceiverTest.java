package com.github.eupedroosouza.messaging.messenger;

import com.github.eupedroosouza.messaging.JedisMockServer;
import com.github.eupedroosouza.messaging.receiver.binary.RPCByteArrayMessageReceiver;
import com.github.eupedroosouza.messaging.sender.binary.RPCByteArrayChannelSender;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RPCByteArrayMessageSenderReceiverTest {

    private static final Logger LOGGER = Logger.getLogger(RPCByteArrayMessageSenderReceiverTest.class.getName());

    private RPCByteArrayChannelSender rpcByteArrayChannelSender;
    private RPCByteArrayMessageReceiver rpcByteArrayMessageReceiver;

    @BeforeAll
    void start() throws IOException {
        JedisMockServer jedisMockServer = JedisMockServer.getInstance();
        rpcByteArrayChannelSender = new RPCByteArrayChannelSender(jedisMockServer.jedisExecutions, "rpc:bam",
                (channel, subscribedChannel) -> LOGGER.info(String.format("Sender Subscribed in channel %s.",  channel)),
                (channel, subscribedChannel)  -> LOGGER.info(String.format("Sender Unsubscribed in channel %s.",  channel)));
        rpcByteArrayChannelSender.start();
        rpcByteArrayMessageReceiver = new RPCByteArrayMessageReceiver(jedisMockServer.jedisExecutions, "rpc:bam",
                (channel, subscribedChannel) -> LOGGER.info(String.format("Receiver Subscribed in channel %s.", channel)),
                (channel, subscribedChannel) -> LOGGER.info(String.format("Receiver Unsubscribed in channel %s.", channel))) {
            @Override
            public CompletableFuture<byte[]> receive(byte[] message) {
                return CompletableFuture.completedFuture("Response".getBytes());
            }
        };
        rpcByteArrayMessageReceiver.start();
    }

    @Test
    @Order(1)
    void send() {
        assertEquals(new String(rpcByteArrayChannelSender.send(new byte[] {}).join().getResponse(), StandardCharsets.UTF_8), "Response");
    }

    @AfterAll
    void stop() {
        rpcByteArrayChannelSender.shutdown();
        rpcByteArrayMessageReceiver.shutdown();
    }

}
