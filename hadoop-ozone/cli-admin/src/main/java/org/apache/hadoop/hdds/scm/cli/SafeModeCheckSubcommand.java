/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.cli;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.ozone.ha.ConfUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * This is the handler that process safe mode check command.
 */
@Command(
    name = "status",
    description = "Check if SCM is in safe mode",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class SafeModeCheckSubcommand extends ScmSubcommand {
  @CommandLine.Option(names = {"--all", "-a"},
      description = "Show safe mode status for all SCM nodes in the service. " +
          "When multiple SCM service IDs are configured, --service-id must be specified.")
  private boolean allNodes;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (allNodes) {
      executeForAllNodes(scmClient);
    } else {
      executeForSingleNode(scmClient);
    }
  }

  private void executeForSingleNode(ScmClient scmClient) throws IOException {
    try {
      boolean execReturn = scmClient.inSafeMode();
      if (execReturn) {
        System.out.println("SCM is in safe mode.");
      } else {
        System.out.println("SCM is out of safe mode.");
      }
      if (isVerbose()) {
        printSafeModeRuleStatus(scmClient);
      }
    } catch (Exception e) {
      System.out.println("STATUS NOT AVAILABLE");
      if (isVerbose()) {
        System.out.println("Reason: " + e.getMessage());
      }
    }
  }

  private void executeForAllNodes(ScmClient scmClient) throws IOException {
    final OzoneConfiguration conf = getOzoneConf();
    String serviceId = HddsUtils.getScmServiceId(conf);

    if (serviceId == null) {
      executeForSingleNode(scmClient);
      return;
    }

    System.out.println("Service ID: " + serviceId);
    List<SCMNodeInfo> nodes = SCMNodeInfo.buildNodeInfo(conf);

    if (nodes.isEmpty()) {
      System.out.println("No nodes configured for service: " + serviceId);
      return;
    }

    for (SCMNodeInfo node : nodes) {
      OzoneConfiguration nodeConf = new OzoneConfiguration(conf);
      String nodesKey = ConfUtils.addKeySuffixes(
          ScmConfigKeys.OZONE_SCM_NODES_KEY, serviceId);
      nodeConf.set(nodesKey, node.getNodeId());

      try (ScmClient perNode = new ContainerOperationClient(nodeConf)) {
        boolean inSafeMode = perNode.inSafeMode();
        System.out.printf("%s [%s]: %s%n",
            node.getScmClientAddress(),
            node.getNodeId(),
            inSafeMode ? "IN SAFE MODE" : "OUT OF SAFE MODE");

        if (isVerbose()) {
          printSafeModeRuleStatus(perNode);
          System.out.println();
        }
      } catch (Exception e) {
        System.out.printf("%s [%s]: STATUS NOT AVAILABLE%n",
            node.getScmClientAddress(), node.getNodeId());
        if (isVerbose()) {
          System.out.println("  Reason: " + e.getMessage());
        }
      }
    }
  }

  /**
   * Print safe mode rule statuses.
   */
  private void printSafeModeRuleStatus(ScmClient scmClient) throws IOException {
    for (Map.Entry<String, Pair<Boolean, String>> entry :
        scmClient.getSafeModeRuleStatuses().entrySet()) {
      Pair<Boolean, String> value = entry.getValue();
      System.out.printf("validated:%s, %s, %s%n",
          value.getLeft(), entry.getKey(), value.getRight());
    }
  }
}
