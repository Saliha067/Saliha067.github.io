---
title: "Kafka 3.9.0: Migrating from ZooKeeper to KRaft Mode"
date: 2025-12-29T10:00:00+05:30
draft: false
mermaid: true
ShowToc: true
TocOpen: false
ShowShareButtons: true
sharingButtons: ["x", "linkedin", "whatsapp", "reddit"]
summary: "Step-by-step guide to migrate Kafka 3.9.0 from ZooKeeper to KRaft mode with zero downtime"
tags: ["kafka", "kraft", "platform-engineering", "docker"]
categories: ["blog"]
cover:
  image: "/images/kafka-zk-to-kraft/coverimage.png"
  alt: ""
  relative: false
---

## Introduction

ZooKeeper has been Kafka's metadata management backbone for over a decade, but it comes with operational complexity: separate processes to maintain, additional monitoring, and another failure point. Apache Kafka 3.3+ introduced **KRaft mode** (Kafka Raft metadata mode) as production-ready, allowing you to run Kafka without ZooKeeper.

**Why migrate to KRaft?**
- **Eliminate ZooKeeper dependency** - One less system to manage and monitor
- **Simpler architecture** - Kafka handles its own metadata via the Raft consensus protocol
- **Better scalability** - Supports millions of partitions (ZooKeeper struggles beyond 200K)
- **Faster recovery** - Controller failover happens in milliseconds instead of seconds

This guide walks you through migrating a Kafka 3.9.0 cluster from ZooKeeper mode to KRaft mode with **zero downtime**. The migration happens in phases, allowing you to rollback at each checkpoint.

**Starting point:** Need a Kafka 3.9.0 cluster with ZooKeeper? See [this upgrade guide](/posts/kafka-370-to-390-upgrade/) for setup instructions.

---

## Migration Overview

The migration happens in 4 distinct phases, each moving the cluster closer to pure KRaft mode:

{{< mermaid >}}
graph TB
    subgraph Phase1["Phase 1: Deploy Controllers"]
        P1_ZK[ZooKeeper<br/>Active]
        P1_B[Brokers<br/>ZK Mode]
        P1_K[KRaft Controllers<br/>Quorum Formed]
        
        P1_B --> P1_ZK
        P1_K -.->|Not Connected| P1_B
        
        style P1_ZK fill:#ffd700
        style P1_B fill:#87ceeb
        style P1_K fill:#90EE90
    end
    
    subgraph Phase2["Phase 2: Enable Migration"]
        P2_ZK[ZooKeeper<br/>Active]
        P2_B[Brokers<br/>Dual-Write Mode]
        P2_K[KRaft Controllers<br/>Syncing Metadata]
        
        P2_B --> P2_ZK
        P2_B -.->|Metadata Sync| P2_K
        
        style P2_ZK fill:#ffd700
        style P2_B fill:#ffa500
        style P2_K fill:#90EE90
    end
    
    subgraph Phase3["Phase 3: Brokers to KRaft"]
        P3_ZK[ZooKeeper<br/>Running but Unused]
        P3_B[Brokers<br/>KRaft Mode Observers]
        P3_K[KRaft Controllers<br/>Managing Metadata]
        
        P3_B --> P3_K
        P3_K -.->|Dual-Write| P3_ZK
        
        style P3_ZK fill:#d3d3d3
        style P3_B fill:#4CAF50
        style P3_K fill:#4CAF50
    end
    
    subgraph Phase4["Phase 4: Finalize"]
        P4_B[Brokers<br/>KRaft Mode Observers]
        P4_K[KRaft Controllers<br/>Pure KRaft]
        
        P4_B --> P4_K
        
        style P4_B fill:#4CAF50
        style P4_K fill:#4CAF50
    end
    
    Phase1 -->|Rolling Restart| Phase2
    Phase2 -->|Config Update| Phase3
    Phase3 -->|Decommission ZK| Phase4
{{< /mermaid >}}

**Key phases:**
- **Phase 1**: Deploy KRaft controllers alongside ZooKeeper
- **Phase 2**: Enable migration - brokers dual-write to both systems
- **Phase 3**: Convert brokers to KRaft mode as observers
- **Phase 4**: Finalize - remove ZooKeeper dependency completely

---

## Architecture Overview

Our migration transforms the cluster architecture from ZooKeeper-based coordination to KRaft-based self-management.

### Before Migration: ZooKeeper Mode

{{< mermaid >}}
graph LR
    subgraph Kafka["Kafka Cluster"]
        K1[Kafka Broker 1<br/>3.9.0]
        K2[Kafka Broker 2<br/>3.9.0]
        K3[Kafka Broker 3<br/>3.9.0]
    end
    
    subgraph ZooKeeper["ZooKeeper Ensemble"]
        ZK1[ZooKeeper 1]
        ZK2[ZooKeeper 2]
        ZK3[ZooKeeper 3]
    end
    
    Kafka --> ZooKeeper
    
    style ZK1 fill:#ffd700
    style ZK2 fill:#ffd700
    style ZK3 fill:#ffd700
{{< /mermaid >}}

**Current state:**
- 3 ZooKeeper nodes forming an ensemble with quorum-based consensus
- 3 Kafka brokers (version 3.9.0) connecting to the ZooKeeper ensemble
- All coordination, leader elections, and configuration stored in ZooKeeper

### After Migration: KRaft Mode

{{< mermaid >}}
graph LR
    subgraph Controllers["KRaft Controller Quorum (Voters)"]
        C1[Controller 1<br/>Leader]
        C2[Controller 2<br/>Follower]
        C3[Controller 3<br/>Follower]
    end
    
    subgraph Brokers["Kafka Brokers (Observers)"]
        B1[Broker 1]
        B2[Broker 2]
        B3[Broker 3]
    end
    
    C1 ---|Raft Protocol| C2
    C2 ---|Raft Protocol| C3
    C3 ---|Raft Protocol| C1
    
    Controllers -.->|Metadata Updates| Brokers
    
    style C1 fill:#2E7D32,color:#fff
    style C2 fill:#4CAF50,color:#fff
    style C3 fill:#4CAF50,color:#fff
    style B1 fill:#1976D2,color:#fff
    style B2 fill:#1976D2,color:#fff
    style B3 fill:#1976D2,color:#fff
{{< /mermaid >}}

**Target state:**
- **3 KRaft controllers** (voters) - Form Raft quorum with 1 leader, 2 followers
- **3 Kafka brokers** (observers) - Handle client requests, observe metadata
- **No ZooKeeper** - Metadata managed by KRaft protocol internally
- **Event-driven** - Metadata changes propagate via __cluster_metadata topic

---

## Initial Setup

This section covers setting up a Kafka 3.9.0 cluster with ZooKeeper from scratch. If you already have a running cluster, skip to [Phase 1](#phase-1-deploy-kraft-controllers).

### Build Docker Images

**What happens:** Build the base image with Kafka 3.9.0 binaries, then create specialized images for ZooKeeper, Kafka brokers, and KRaft controllers.

```bash
cd kafka3.9.0
./docker-image-build.sh
```

**Expected:** Four images created:
- `kafka-lab-base` - Base image with Kafka 3.9.0 binaries
- `kafka-lab-zk` - ZooKeeper image
- `kafka-lab-kafka` - Kafka broker image
- `kafka-lab-kraft` - KRaft controller image

### Start Containers

**What happens:** Launch all 6 containers (3 ZooKeeper + 3 Kafka brokers) using Docker Compose with persistent volumes and custom network.

```bash
docker-compose up -d
sleep 10
docker ps
```

**Expected:** All 6 containers running:
```
CONTAINER ID   IMAGE                  STATUS         PORTS                    NAMES
abc123...      kafka-lab-zk           Up 10 seconds  2181/tcp                 zk1
def456...      kafka-lab-zk           Up 10 seconds  2181/tcp                 zk2
ghi789...      kafka-lab-zk           Up 10 seconds  2181/tcp                 zk3
jkl012...      kafka-lab-kafka        Up 10 seconds  9092/tcp                 kafka1
mno345...      kafka-lab-kafka        Up 10 seconds  9092/tcp                 kafka2
pqr678...      kafka-lab-kafka        Up 10 seconds  9092/tcp                 kafka3
```

### Initialize ZooKeeper Ensemble

**What happens:** Configure each ZooKeeper node with its unique ID and server list, then start the ZooKeeper service on all nodes.

```bash
for i in 1 2 3; do 
    echo "Starting zookeeper node zk$i"
    docker exec zk$i bash -c 'echo $ZOO_MY_ID > /zookeeper/data/myid' 
    docker exec zk$i bash -c 'echo $ZOO_SERVERS | tr " " "\n" >> /opt/kafka/config/zoo.cfg'
    docker exec zk$i systemctl start zookeeper
done

sleep 5
```

**Verify ZooKeeper is running:**

```bash
docker exec zk1 bash -c 'echo ruok | nc localhost 2181'
```

**Expected:**
```
imok
```

```bash
docker exec zk1 systemctl status zookeeper | grep "Active:"
```

**Expected:**
```
Active: active (running) since...
```

### Initialize Kafka Brokers

**What happens:** Generate configuration for each broker using environment variables, then start the Kafka service on all brokers.

```bash
for i in 1 2 3; do \
    echo "Starting kafka broker node kafka$i" && \
    docker exec kafka$i bash -c 'sh /var/tmp/kafka_config_generator.sh' && \
    docker exec kafka$i systemctl start kafka
done

sleep 10
```

**Verify all brokers registered with ZooKeeper:**

```bash
docker exec zk1 /opt/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids
```

**Expected:**
```
[1, 2, 3]
```

### Verify Kafka Version

**What happens:** Confirm all brokers are running Kafka 3.9.0 before starting the migration.

```bash
docker exec kafka1 ls /opt/kafka/libs | grep kafka_
```

**Expected:**
```
kafka_2.13-3.9.0.jar
```

### Create Test Topic

**What happens:** Create a test topic with 3 partitions and replication factor 3 to verify cluster health throughout the migration.

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1.local:9092 \
  --create --topic test \
  --partitions 3 \
  --replication-factor 3
```

**Describe the test topic:**

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1.local:9092 \
  --describe --topic test
```

**Expected:**
```
Topic: test     TopicId: xyz123     PartitionCount: 3       ReplicationFactor: 3
Topic: test     Partition: 0    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
Topic: test     Partition: 1    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1
Topic: test     Partition: 2    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2
```

**What to verify:**
- ✅ 3 partitions created
- ✅ All replicas in sync (ISR matches Replicas)
- ✅ Leaders distributed across brokers

---

## Phase 1: Deploy KRaft Controllers

**What happens:** Start 3 KRaft controller nodes that will form a Raft quorum. These controllers will eventually replace ZooKeeper for metadata management. The cluster continues running on ZooKeeper during this phase.

### Get Cluster ID from ZooKeeper

**What happens:** Extract the existing cluster UUID from ZooKeeper. KRaft controllers must use the same cluster ID to ensure continuity.

```bash
CLUSTER_ID=$(docker exec zk1 /opt/kafka/bin/zookeeper-shell.sh localhost:2181 \
  get /cluster/id 2>&1 | grep '"id"' | sed 's/.*"id":"\([^"]*\)".*/\1/')

echo "Cluster ID: $CLUSTER_ID"
```

**Expected:**
```
Cluster ID: SnA2-fqPTM-_QGpKAiA8oA
```

**Caution:** This cluster ID must match exactly. If empty or malformed, check ZooKeeper connectivity.

### Start KRaft Controllers

**What happens:** Launch 3 KRaft controller containers using the cluster ID from ZooKeeper. Each controller starts with the `/var/tmp/start-kraft.sh` script.

```bash
for i in 1 2 3; do 
  echo "Starting kraft$i..."
  docker exec -d -e CLUSTER_UUID=$CLUSTER_ID kraft$i /var/tmp/start-kraft.sh
  sleep 5
done

sleep 20
```

**Why the wait?** Controllers need time to:
1. Format storage directories (5 seconds)
2. Start Kafka process (5 seconds)
3. Form quorum and elect leader (10 seconds)

### Verify Quorum Formation

**What happens:** Check that the 3 controllers have formed a Raft quorum with 1 leader and 2 followers.

```bash
docker exec kraft1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller kraft1:9093 describe --replication --human-readable
```

**Expected:**
```
NodeId  DirectoryId             LogEndOffset    Lag     LastFetchTimestamp      LastCaughtUpTimestamp   Status
101     KfL1aMqPSuGz1bX2cY3dEQ  156             0       5 ms ago                5 ms ago                Leader
102     GhI2bNrQTvHz2cY3dZ4eFg  156             0       215 ms ago              215 ms ago              Follower
103     JkL3c0sRUwIa3dZ4eA5fGg  156             0       215 ms ago              215 ms ago              Follower
```

**What to verify:**
- ✅ 3 controllers present (IDs: 101, 102, 103)
- ✅ 1 Leader, 2 Followers
- ✅ LogEndOffset matches across all nodes
- ✅ Lag is 0 (all controllers caught up)

**Current state:** ZooKeeper still active, brokers still using ZooKeeper, KRaft controllers running in parallel (not yet managing brokers).

---

## Phase 2: Enable Migration on Brokers

**What happens:** Configure brokers to start dual-writing metadata to both ZooKeeper (old) and KRaft controllers (new). This phase synchronizes metadata between the two systems.

### Add Migration Configuration

**What happens:** Add migration-specific settings to each broker's configuration file, enabling them to communicate with the KRaft controller quorum.

```bash
MIGRATION_CONFIG='listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
zookeeper.metadata.migration.enable=true
controller.quorum.bootstrap.servers=kraft1:9093,kraft2:9093,kraft3:9093
controller.listener.names=CONTROLLER'

for i in 1 2 3; do
  docker exec kafka$i bash -c "echo '$MIGRATION_CONFIG' >> /opt/kafka/config/server.properties"
  echo "Added migration config to kafka$i"
done
```

**Configuration explained:**
- `listener.security.protocol.map` - Defines CONTROLLER listener protocol
- `zookeeper.metadata.migration.enable=true` - Activates dual-write mode
- `controller.quorum.bootstrap.servers` - KRaft controller addresses
- `controller.listener.names=CONTROLLER` - Listener name for controller communication

### Rolling Restart Brokers

**What happens:** Restart each broker one by one to apply the migration configuration. During restart, metadata starts syncing to KRaft controllers.

```bash
for i in 1 2 3; do
  echo "Restarting kafka$i..."
  docker exec kafka$i systemctl restart kafka
  sleep 20
  echo "kafka$i restarted."
done

sleep 10
```

**Why 20 seconds between restarts?** Allows broker to:
1. Shut down gracefully (5 seconds)
2. Start up and rejoin cluster (10 seconds)
3. Begin metadata sync to KRaft (5 seconds)

### Verify Migration Completed

**What happens:** Check broker logs to confirm metadata has been fully synchronized from ZooKeeper to KRaft controllers.

```bash
docker exec kraft1 grep "Completed migration" /opt/kafka/logs/server.log
```

**Expected:**
```
[2025-12-29 10:15:23,456] INFO Completed migration of metadata from ZooKeeper to KRaft (kafka.migration.ZkMigrationClient)
```

**What to verify:**
- ✅ "Completed migration" log entry appears
- ✅ No error messages in controller logs
- ✅ All brokers successfully restarted

**Current state:** Brokers dual-writing to both ZooKeeper and KRaft controllers. Metadata synchronized. Still using ZooKeeper as primary coordination system.

---

## Phase 3: Move Brokers to KRaft Mode

**What happens:** Convert brokers from ZooKeeper-mode to KRaft-mode by updating their configuration to read metadata exclusively from KRaft controllers. This removes ZooKeeper dependency from brokers.

### Update Broker Configurations

**What happens:** Change broker configurations to operate in KRaft mode as "brokers" (not voters in the quorum, just observers).

```bash
for i in 1 2 3; do
  docker exec kafka$i bash -c "
    sed -i 's/broker.id/node.id/g' /opt/kafka/config/server.properties
    echo 'process.roles=broker' >> /opt/kafka/config/server.properties
    sed -i '/inter.broker.protocol.version/d' /opt/kafka/config/server.properties
    sed -i '/zookeeper.connect/d' /opt/kafka/config/server.properties
    sed -i '/zookeeper.metadata.migration.enable/d' /opt/kafka/config/server.properties
    sed -i '/zookeeper.session.timeout.ms/d' /opt/kafka/config/server.properties
  "
  echo "Updated kafka$i configuration"
done
```

**Configuration changes:**
- `broker.id` → `node.id` - KRaft uses node.id for all nodes
- Add `process.roles=broker` - Declares this node as broker (not controller)
- Remove `inter.broker.protocol.version` - Not needed in KRaft mode
- Remove `zookeeper.connect` - No longer connecting to ZooKeeper
- Remove `zookeeper.metadata.migration.enable` - Migration complete
- Remove `zookeeper.session.timeout.ms` - No ZooKeeper sessions

### Rolling Restart Brokers

**What happens:** Restart brokers one by one to activate KRaft-mode configuration. Brokers now read metadata from KRaft controllers as observers.

```bash
for i in 1 2 3; do
  echo "Restarting kafka$i in KRaft mode..."
  docker exec kafka$i systemctl restart kafka
  sleep 20
  echo "kafka$i now in KRaft mode."
done
```

### Verify Brokers as Observers

**What happens:** Confirm brokers appear in the KRaft quorum as observers (non-voting members that read metadata).

```bash
docker exec kraft1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller kraft1:9093 describe --status
```

**Expected:**
```
ClusterId:              SnA2-fqPTM-_QGpKAiA8oA
LeaderId:               101
LeaderEpoch:            3
HighWatermark:          1853
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [{"id": 101, ...}, {"id": 102, ...}, {"id": 103, ...}]
CurrentObservers:       [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]
```

**What to verify:**
- ✅ CurrentVoters shows 3 controllers (IDs: 101, 102, 103)
- ✅ CurrentObservers shows 3 brokers (IDs: 1, 2, 3)
- ✅ MaxFollowerLag is 0

### Verify No Under-Replicated Partitions

**What happens:** Ensure all topic partitions are healthy and fully replicated after the configuration change.

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1.local:9092 \
  --describe --under-replicated-partitions
```

**Expected:** Empty output (no under-replicated partitions)

**Current state:** Brokers operating in KRaft mode as observers, reading metadata from controllers. ZooKeeper still running but **no longer used by brokers**.

---

## Phase 4: Finalize Migration

**What happens:** Remove ZooKeeper dependency from KRaft controllers and decommission ZooKeeper entirely. This completes the migration to pure KRaft mode.

### Remove ZooKeeper Config from Controllers

**What happens:** Clean up ZooKeeper-related configuration from controller properties files.

```bash
for i in 1 2 3; do
  docker exec kraft$i bash -c "
    sed -i '/zookeeper.connect/d' /opt/kafka/config/kraft/controller.properties
    sed -i '/zookeeper.metadata.migration.enable/d' /opt/kafka/config/kraft/controller.properties
  "
  echo "Removed ZooKeeper config from kraft$i"
done
```

### Rolling Restart Controllers

**What happens:** Restart controllers one by one to apply the configuration changes. Controllers now operate in pure KRaft mode.

```bash
for i in 1 2 3; do
  docker exec kraft$i pkill -f kafka
  sleep 3
  docker exec -d -e CLUSTER_UUID=$CLUSTER_ID kraft$i /var/tmp/start-kraft.sh
  sleep 15
  echo "kraft$i restarted in pure KRaft mode."
done
```

**Why 15 seconds?** Allows controller to:
1. Shut down gracefully (3 seconds)
2. Start up (5 seconds)
3. Rejoin quorum (7 seconds)

### Decommission ZooKeeper

**What happens:** Stop all ZooKeeper services as they are no longer needed.

```bash
for i in 1 2 3; do
  echo "Stopping zookeeper node zk$i"
  docker exec zk$i systemctl stop zookeeper 2>/dev/null || docker exec zk$i pkill -f zookeeper
  sleep 2
done
```

**Verify ZooKeeper is stopped:**

```bash
for i in 1 2 3; do
  STATUS=$(docker exec zk$i ps aux | grep -E "zookeeper|QuorumPeerMain" | grep -v grep || echo "No ZooKeeper process")
  echo "zk$i: $STATUS"
done
```

**Expected:**
```
zk1: No ZooKeeper process
zk2: No ZooKeeper process
zk3: No ZooKeeper process
```

### Verify Final Quorum Status

**What happens:** Confirm the KRaft quorum is healthy with all controllers and broker observers active.

```bash
docker exec kraft1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller kraft1:9093 describe --replication --human-readable
```

**Expected:**
```
NodeId  DirectoryId             LogEndOffset    Lag     LastFetchTimestamp      LastCaughtUpTimestamp   Status
101     KfL1aMqPSuGz1bX2cY3dEQ  2718            0       6 ms ago                6 ms ago                Leader
102     GhI2bNrQTvHz2cY3dZ4eFg  2718            0       226 ms ago              226 ms ago              Follower
103     JkL3c0sRUwIa3dZ4eA5fGg  2718            0       226 ms ago              226 ms ago              Follower
1       S6hsGkwXxgwPaz8JQTzFCQ  2718            0       226 ms ago              226 ms ago              Observer
2       IEpkgBT6ucGSzwaZm53U-g  2718            0       226 ms ago              226 ms ago              Observer
3       UlEhjluFhWjj5EnHeMD4-g  2718            0       226 ms ago              226 ms ago              Observer
```

**What to verify:**
- ✅ 3 voters (controllers 101, 102, 103)
- ✅ 3 observers (brokers 1, 2, 3)
- ✅ All nodes at same LogEndOffset
- ✅ Lag is 0

### Test Producer/Consumer

**What happens:** End-to-end test to verify the cluster functions correctly in pure KRaft mode.

**Create a new test topic:**

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1.local:9092 \
  --create --topic migration-test --partitions 1 --replication-factor 3

sleep 2
```

**Produce a message:**

```bash
docker exec kafka1 bash -c 'echo "hello-kraft" | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1.local:9092 --topic migration-test'

echo "Message produced"
```

**Consume the message:**

```bash
docker exec kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1.local:9092 --topic migration-test \
  --from-beginning --max-messages 1 --property print.timestamp=true
```

**Expected:**
```
CreateTime:1735450523456    hello-kraft
Processed a total of 1 messages
```

### Verification Summary

| Phase | Check | Command | Expected |
|-------|-------|---------|----------|
| Phase 1 | Quorum formed | `kafka-metadata-quorum.sh describe --replication --human-readable` | 1 Leader, 2 Followers |
| Phase 2 | Migration completed | `grep "Completed migration" /opt/kafka/logs/server.log` | Log entry found |
| Phase 2 | No under-replicated | `kafka-topics.sh --describe --under-replicated-partitions` | Empty output |
| Phase 3 | Brokers as Observers | `kafka-metadata-quorum.sh describe --status` | 3 Voters, 3 Observers |
| Phase 4 | Producer/Consumer | Test message flow | Message received |

**🎉 Migration Complete!** Your Kafka cluster now runs in pure KRaft mode with no ZooKeeper dependency.

---

## Rollback Procedures

**Important:** Rollback becomes increasingly difficult as you progress through phases. Test thoroughly at each phase before proceeding.

### Rollback from Phase 1

**When:** You've deployed KRaft controllers but haven't enabled migration on brokers yet.

**Risk level:** Low - brokers still fully on ZooKeeper

```bash
# Simply stop the KRaft controllers
for i in 1 2 3; do 
  docker exec kraft$i pkill -f kafka
done
```

**Result:** Cluster continues operating normally on ZooKeeper.

### Rollback from Phase 2

**When:** You've enabled migration on brokers but haven't converted them to KRaft mode yet.

**Risk level:** Medium - requires cleaning up migration state

```bash
# Stop KRaft controllers
for i in 1 2 3; do 
  docker exec kraft$i pkill -f kafka
done

# Remove ZK migration znodes
docker exec zk1 /opt/kafka/bin/zookeeper-shell.sh localhost:2181 deleteall /controller
docker exec zk1 /opt/kafka/bin/zookeeper-shell.sh localhost:2181 deleteall /migration

# Remove migration config from brokers
for i in 1 2 3; do
  docker exec kafka$i bash -c "
    sed -i '/listener.security.protocol.map/d' /opt/kafka/config/server.properties
    sed -i '/zookeeper.metadata.migration.enable/d' /opt/kafka/config/server.properties
    sed -i '/controller.quorum.bootstrap.servers/d' /opt/kafka/config/server.properties
    sed -i '/controller.listener.names/d' /opt/kafka/config/server.properties
  "
done

# Rolling restart brokers
for i in 1 2 3; do 
  docker exec kafka$i systemctl restart kafka
  sleep 20
done
```

**Result:** Cluster back to pure ZooKeeper mode.

### Rollback from Phase 3

**When:** You've converted brokers to KRaft mode but haven't finalized controllers yet.

**Risk level:** High - complex rollback, potential for data issues

```bash
# Revert broker configs to ZK mode
for i in 1 2 3; do
  docker exec kafka$i bash -c "
    sed -i 's/node.id/broker.id/g' /opt/kafka/config/server.properties
    sed -i '/process.roles/d' /opt/kafka/config/server.properties
    echo 'zookeeper.connect=zk1.local:2181,zk2.local:2181,zk3.local:2181' >> /opt/kafka/config/server.properties
    echo 'zookeeper.metadata.migration.enable=true' >> /opt/kafka/config/server.properties
  "
done

# Rolling restart brokers
for i in 1 2 3; do 
  docker exec kafka$i systemctl restart kafka
  sleep 20
done

# Then follow Phase 2 rollback to fully return to ZooKeeper
```

**Result:** Brokers back in migration mode. Follow Phase 2 rollback to complete.

### ⚠️ Phase 4: No Easy Rollback

Once you've finalized migration (Phase 4), rolling back requires:
1. Restoring ZooKeeper data from backups
2. Rebuilding broker configurations
3. Potential data loss if metadata diverged

**Recommendation:** Run Phase 4 only after thoroughly testing Phases 1-3 in production for several days.

---

## Production Mitigations

Apply these settings **before** starting the migration to prevent common issues:

### Disable Automatic Leader Rebalancing

**Why:** Preferred Leader Election (PLE) during rolling restarts can cause timeouts and high load.

```bash
# Add to all brokers BEFORE Phase 1
for i in 1 2 3; do
  docker exec kafka$i bash -c "echo 'auto.leader.rebalance.enable=false' >> /opt/kafka/config/server.properties"
done

# Restart brokers to apply
for i in 1 2 3; do
  docker exec kafka$i systemctl restart kafka
  sleep 20
done
```

### Disable Unclean Leader Election

**Why:** Prevents data loss if replicas fall out of sync during migration.

```bash
# Should already be set, but verify
for i in 1 2 3; do
  docker exec kafka$i bash -c "grep 'unclean.leader.election.enable' /opt/kafka/config/server.properties || echo 'unclean.leader.election.enable=false' >> /opt/kafka/config/server.properties"
done
```

### Common Production Issues

| Issue | Symptom | Mitigation |
|-------|---------|------------|
| Application Timeouts During Phase 2/3 | Producer/consumer timeout errors | Set `auto.leader.rebalance.enable=false` |
| OutOfOrderSequenceException | Producer errors during migration | Use default producer retry settings |
| Under-Replicated Partitions | ISR shrinks during restarts | Wait longer between broker restarts (30-60s) |
| Controller Failover | Slow controller election | Ensure 3 controllers, check network latency |

---

## Cleanup

After migration is complete and verified, you can optionally remove ZooKeeper containers:

```bash
# Stop all containers
docker-compose down -v

# Remove all images
docker rmi kafka-lab-base kafka-lab-zk kafka-lab-kafka kafka-lab-kraft
```

---

## Conclusion

You've successfully migrated your Kafka 3.9.0 cluster from ZooKeeper to KRaft mode! Your cluster now:
- Runs without ZooKeeper dependency
- Uses Raft consensus for metadata management
- Has simpler architecture with fewer moving parts
- Supports better scalability for future growth

**Next steps:**
- Monitor cluster health for 24-48 hours
- Re-enable `auto.leader.rebalance.enable=true` if desired
- Update monitoring dashboards to track KRaft metrics
- Plan ZooKeeper infrastructure decommissioning

---

### Get the Full Lab Environment

<div style="max-width: 450px; margin: 2rem auto; padding: 1.5rem; border: 2px solid #3b82f6; border-radius: 8px; background: #eff6ff;">

<div id="kafkaKraftFormContainer">
<p style="margin-bottom: 1rem; font-weight: 600; text-align: center;">📧 Enter your email to access the complete Kafka KRaft migration lab setup</p>

<form id="kafkaKraftSourceCodeForm" action="https://formspree.io/f/xgowjqol" method="POST" style="display: flex; flex-direction: column; gap: 0.75rem;">
  <input type="email" name="email" placeholder="your.email@example.com" required 
         style="padding: 0.75rem; border: 1px solid #cbd5e0; border-radius: 4px; font-size: 1rem;">
  <input type="hidden" name="page" value="kafka-kraft-migration-lab">
  <button type="submit" 
          style="padding: 0.75rem; background: #3b82f6; color: white; border: none; border-radius: 4px; font-weight: 500; cursor: pointer;">
    Get Lab Environment →
  </button>
</form>

<p style="margin-top: 0.75rem; font-size: 0.875rem; color: #64748b; text-align: center;">
Free • Docker setup • Complete scripts
</p>
</div>

<div id="kafkaKraftSuccessMessage" style="display: none; text-align: center;">
<p style="font-size: 2rem; margin-bottom: 0.5rem;">✅</p>
<p style="font-weight: 600; margin-bottom: 0.5rem;">Thanks! Access Granted</p>
<p style="font-size: 0.875rem; color: #64748b;">The repository has been opened in a new tab.</p>
<p style="margin-top: 1rem; font-size: 0.875rem; color: #64748b;">Don't forget to ⭐ star the repo!</p>
</div>

</div>

<script>
document.getElementById('kafkaKraftSourceCodeForm').addEventListener('submit', function(e) {
  e.preventDefault();
  
  const form = e.target;
  const formData = new FormData(form);
  
  fetch(form.action, {
    method: 'POST',
    body: formData,
    headers: {
      'Accept': 'application/json'
    }
  }).then(response => {
    if (response.ok) {
      window.open('https://github.com/Saliha067/kafka-lab/tree/master/kafka3.9.0', '_blank');
      document.getElementById('kafkaKraftFormContainer').style.display = 'none';
      document.getElementById('kafkaKraftSuccessMessage').style.display = 'block';
    } else {
      alert('Submission failed. Please try again.');
    }
  }).catch(error => {
    window.open('https://github.com/Saliha067/kafka-lab/tree/master/kafka3.9.0', '_blank');
    document.getElementById('kafkaKraftFormContainer').style.display = 'none';
    document.getElementById('kafkaKraftSuccessMessage').style.display = 'block';
  });
});
</script>

---

## References

- [Apache Kafka KRaft Documentation](https://kafka.apache.org/documentation/#kraft)
- [KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
- [Kafka 3.9.0 Upgrade Guide](/posts/kafka-370-to-390-upgrade/)
- [Kafka Migration Guide](https://kafka.apache.org/documentation/#kraft_zk_migration)
- [Strimzi: From ZooKeeper to KRaft Migration](https://strimzi.io/blog/2024/03/21/kraft-migration/)
