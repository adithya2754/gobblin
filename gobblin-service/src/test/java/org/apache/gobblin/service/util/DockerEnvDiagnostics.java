/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gobblin.service.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

/**
 * JVM-side diagnostics for debugging Testcontainers "no valid Docker environment" failures.
 *
 * <p>This logs what the JVM can see (env vars, system properties, docker.sock permissions),
 * and attempts to run {@code docker version/info} via {@link ProcessBuilder}.</p>
 */
public final class DockerEnvDiagnostics {

  private static final List<String> ENV_KEYS_TO_LOG = Arrays.asList(
      "DOCKER_HOST",
      "DOCKER_API_VERSION",
      "DOCKER_CONTEXT",
      "DOCKER_CONFIG",
      "DOCKER_CERT_PATH",
      "DOCKER_TLS_VERIFY",
      "TESTCONTAINERS_HOST_OVERRIDE",
      "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
      "TESTCONTAINERS_RYUK_DISABLED",
      "HOME",
      "USER",
      "PATH"
  );

  private DockerEnvDiagnostics() { }

  public static void log(Logger log) {
    try {
      log.info("[DockerEnvDiagnostics] os.name={} os.version={} java.version={} user.name={}",
          System.getProperty("os.name"),
          System.getProperty("os.version"),
          System.getProperty("java.version"),
          System.getProperty("user.name"));

      Map<String, String> env = System.getenv();
      for (String key : ENV_KEYS_TO_LOG) {
        log.info("[DockerEnvDiagnostics] env {}={}", key, env.get(key));
      }

      File dockerSock = new File("/var/run/docker.sock");
      log.info("[DockerEnvDiagnostics] docker.sock exists={} isFile={} canRead={} canWrite={} path={}",
          dockerSock.exists(), dockerSock.isFile(), dockerSock.canRead(), dockerSock.canWrite(), dockerSock.getAbsolutePath());

      runAndLog(log, "docker", "version");
      runAndLog(log, "docker", "info");
      runAndLog(log, "docker", "ps");
    } catch (Throwable t) {
      // Never fail tests because diagnostics couldn't run.
      log.warn("[DockerEnvDiagnostics] diagnostics failed", t);
    }
  }

  private static void runAndLog(Logger log, String... command) {
    try {
      Process proc = new ProcessBuilder(command).redirectErrorStream(true).start();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          log.info("[DockerEnvDiagnostics] $ {} | {}", String.join(" ", command), line);
        }
      }
      int exit = proc.waitFor();
      log.info("[DockerEnvDiagnostics] $ {} exited with code {}", String.join(" ", command), exit);
    } catch (Throwable t) {
      log.warn("[DockerEnvDiagnostics] failed running command: {}", String.join(" ", command), t);
    }
  }
}

