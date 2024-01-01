/*
 * Copyright (c) 2024 Pedro Souza
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */

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
