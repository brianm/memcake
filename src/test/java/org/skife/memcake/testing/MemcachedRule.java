/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.memcake.testing;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class MemcachedRule extends ExternalResource {
    private static final Logger log = LoggerFactory.getLogger(MemcachedRule.class);

    private StartedProcess process;
    private int port;

    @Override
    protected void before() throws Throwable {
        this.port = findUnusedPort();
        this.process = new ProcessExecutor().command("memcached", "-p", port + "")
                                            .redirectOutput(Slf4jStream.of(log).asDebug())
                                            .redirectError(Slf4jStream.of(log).asInfo())
                                            .start();
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 2000) {
            Socket sock = new Socket();
            try {
                sock.connect(getAddress(), 100);
                sock.close();
                return;
            } catch (IOException e) {
                // keep trying
            }
        }

    }

    @Override
    protected void after() {
        process.getProcess().destroyForcibly();
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(port);
    }

    public static synchronized int findUnusedPort() {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(0));
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException e) {
            throw new RuntimeException("Unable to bind random high port", e);
        }
    }
}
