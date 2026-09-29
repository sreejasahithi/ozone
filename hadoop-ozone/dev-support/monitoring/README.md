# Auto-Balancer Script — Setup and Testing

## Prerequisites

The following instructions assume that the Ozone distribution is available at:

```text
/<path>/ozone/hadoop-ozone/dist/target/ozone-2.3.0-SNAPSHOT/compose/ozone
```

---

## Step 1: Configure the Environment

Navigate to the Ozone Compose directory:

```bash
cd /<path>/ozone/hadoop-ozone/dist/target/ozone-2.3.0-SNAPSHOT/compose/ozone
```

Set the required environment variables:

```bash
export COMPOSE_FILE=docker-compose.yaml:monitoring.yaml:docker-compose.demo-volumes.yaml
export OZONE_SAFEMODE_MIN_DATANODES=3
export OZONE_REPLICATION_FACTOR=3
```

---

## Step 2: Start the Ozone Cluster

Start the cluster without automatically scaling additional DataNodes:

```bash
docker compose up -d --scale datanode=0
```

---

## Step 3: Copy the Auto-Balancer Files

Open a shell in the Ozone Manager container:

```bash
docker compose exec om bash
```

Create the following files inside the container and copy the corresponding script/configuration contents into them:

```bash
vi /tmp/auto-balancer.py
vi /tmp/auto-balancer.config.json
```

The files should be available at:

```text
/tmp/auto-balancer.py
/tmp/auto-balancer.config.json
```

---

# Creating Data Imbalance

To create an imbalance that can be detected by the Auto-Balancer script, put two DataNodes into maintenance.

For example:

```bash
ozone admin datanode maintenance dn1 dn2
```

Check the DataNode status:

```bash
ozone admin datanode list
```

Wait until the selected DataNodes are in maintenance.

## Generate Load

Create a 40 MB test file:

```bash
dd if=/dev/urandom of=/tmp/demo40mb bs=1M count=40
```

You can run this command up to three times if additional load is required.

Create an Ozone volume:

```bash
ozone sh volume create /demo
```

Create a bucket with THREE replication:

```bash
ozone sh bucket create --replication THREE --type RATIS /demo/bucket
```

Write multiple copies of the test file:

```bash
for i in $(seq 1 8); do
    ozone sh key put /demo/bucket/file-$i /tmp/demo40mb
    echo "wrote file-$i"
done
```

## Recommission the DataNodes

After creating the imbalance, make sure the required containers are closed before recommissioning the DataNodes.

Then recommission the DataNodes:

```bash
ozone admin datanode recommission dn1 dn2
```

Verify their status:

```bash
ozone admin datanode list
```

Wait until the DataNodes are back in service.

---

# Run the Auto-Balancer Script

Run the script using the configuration file:

```bash
python3 /tmp/auto-balancer.py \
    -c /tmp/auto-balancer.config.json \
    -d /tmp/reports
```

The generated summary reports will be available under:

```text
/tmp/reports
```

---

# Testing Individual Sanity Checks

A sanity check can be intentionally failed by temporarily lowering the threshold for the metric being tested.

For example, to test:

```text
replication_manager_metrics_under_replicated_queue_size
```

temporarily set its threshold in:

```text
/tmp/auto-balancer.config.json
```

to a sufficiently low value so that the current metric value exceeds the threshold.

Then repeat **Step 1**, **Step 2**, and **Step 3** in a new cluster.

---

## Generate Replication Load

You can generate load using Ozone Freon:

```bash
ozone freon ockg -n 5000 -t 8 -s 100KB
```

Run the command twice.

---

## Monitor the Replication Manager Metric

Monitor the metric continuously:

```bash
while true; do
    date -u +%H:%M:%S
    curl -s http://scm:9876/prom \
        | grep replication_manager_metrics_under_replicated_queue_size \
        | grep -v '^#'
    sleep 1
done
```

Wait until the metric value exceeds the temporarily lowered test threshold.

For example, if the configured test threshold is:

```text
10
```

wait until the metric reports a value greater than `10`.

Once the threshold is exceeded, run the Auto-Balancer script:

```bash
python3 /tmp/auto-balancer.py \
    -c /tmp/auto-balancer.config.json \
    -d /tmp/reports
```

The corresponding sanity check should fail, and the result should be reflected in the generated report.

---


