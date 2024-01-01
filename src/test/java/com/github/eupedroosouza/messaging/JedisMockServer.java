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

package com.github.eupedroosouza.messaging;

import com.github.eupedroosouza.messaging.connection.JedisExecutions;
import com.github.eupedroosouza.messaging.connection.executions.UnifiedJedisExecutions;
import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.BeforeAll;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.logging.Logger;

public class JedisMockServer {

    private static final Logger LOGGER = Logger.getLogger(JedisMockServer.class.getName());
    private static JedisMockServer jedisMockServer;

    public final RedisServer server;
    public final JedisPooled jedisPooled;
    public final JedisExecutions jedisExecutions;

    public JedisMockServer() throws IOException {
        server = RedisServer.newRedisServer();
        server.start();
        LOGGER.info(String.format("Jedis Mock Server started Redis Server in host %s and port %d.", server.getHost(), server.getBindPort()));
        jedisPooled = new JedisPooled(server.getHost(), server.getBindPort());
        if (!jedisPooled.ping().equals("PONG"))
            throw new IllegalStateException("Redis Server ping response was unexpected.");
        LOGGER.info(String.format("Connect in Redis Server at %s:%d", server.getHost(), server.getBindPort()));
        jedisExecutions = new UnifiedJedisExecutions(jedisPooled);
    }

    public RedisServer getServer() {
        return server;
    }

    public JedisPooled getJedisPooled() {
        return jedisPooled;
    }

    public JedisExecutions getJedisExecutions() {
        return jedisExecutions;
    }

    public static JedisMockServer getInstance() throws IOException {
        if (jedisMockServer == null)
            jedisMockServer = new JedisMockServer();
        return jedisMockServer;
    }
}
