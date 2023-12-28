/*
 * Copyright � 2023 Pedro Souza
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the �Software�), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.github.eupedroosouza.messaging.sender.binary;

import com.github.eupedroosouza.messaging.connection.BaseJedisConnection;
import com.github.eupedroosouza.messaging.connection.JedisConnectionProvider;
import com.github.eupedroosouza.messaging.message.status.MessageStatus;
import com.github.eupedroosouza.messaging.util.FutureUtil;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class ByteArrayMessageSender extends BaseJedisConnection {

    private final byte[] byteChannel;

    public ByteArrayMessageSender(JedisConnectionProvider connectionProvider, String channel) {
        super(connectionProvider);
        this.byteChannel = channel.getBytes(StandardCharsets.UTF_8);
    }

    public CompletableFuture<MessageStatus> send(byte[] message) {
        return FutureUtil.exceptionAsyncFuture(() -> {
            long status = pubBinary(byteChannel, message);
            if (status == 0)
                return MessageStatus.NOT_SUBSCRIBERS_CHANNEL;
            return MessageStatus.SUCCESS;
        });
    }

}
