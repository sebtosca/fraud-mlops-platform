## 02. Streaming and the scoring service

Researched 2026-10-04. Versions and dates come from PyPI, GitHub releases and project changelogs, fetched on that date.

### Questions

1. Strimzi on kind: which Strimzi version is current (KRaft, no ZooKeeper)? What are minimal single-broker `Kafka`, `KafkaNodePool` and `KafkaTopic` manifests for a laptop? What is the memory footprint? Should it be installed with Helm or the operator YAML? How do clients inside and outside the cluster connect? What changes on GKE?
2. Which Python Kafka client should we use in 2026: confluent-kafka-python, aiokafka or kafka-python? Compare maintenance, Python 3.13 wheels and performance. How does keying by card guarantee per-card ordering? Does the default partitioner differ between clients, and can that cause a mismatch?
3. Stateful consumer patterns: per-card state in memory, snapshot and restore across rebalances (`on_assign`/`on_revoke`), the order of offset commit versus state snapshot, and the trade-off between at-least-once and exactly-once for this demo.
4. Service shape: should this be one FastAPI app (a background consumer plus HTTP `/score`) or two deployables? How do we load the champion and challenger from MLflow by alias and hot-reload them? How do we write predictions to Postgres efficiently?
5. Replay producer on an accelerated clock: how do we replay by event time with a speed-up factor, and how do downstream components read "now" from event time?

### Findings

#### Q1. Strimzi on kind

**Current version.** Strimzi **1.2.0**, released 2026-08-20, supports Apache Kafka **4.3.1** ([GitHub releases API](https://api.github.com/repos/strimzi/strimzi-kafka-operator/releases); [CHANGELOG](https://github.com/strimzi/strimzi-kafka-operator/blob/main/CHANGELOG.md)). The earlier releases were 1.1.0 (2026-06-27) and 1.0.1 (2026-06-17). Version 1.3.0 is unreleased and is listed in the changelog.

Key facts from the changelog ([CHANGELOG.md](https://github.com/strimzi/strimzi-kafka-operator/blob/main/CHANGELOG.md)):
- ZooKeeper is gone. The changelog says: "**Strimzi 0.45 is the last minor Strimzi version with support for ZooKeeper-based Apache Kafka clusters and MirrorMaker 1 deployments.**" It also says: "Support for ZooKeeper-based Apache Kafka clusters and for KRaft migration has been removed" (0.46). Every 1.x cluster is KRaft, with nodes defined by `KafkaNodePool`.
- Only the v1 API remains. 1.0.0 says: "Remove the `v1beta2` API (and `v1alpha1` and `v1beta2` for `KafkaTopic` and `KafkaUser`) from the CRDs and fully move to the `v1` API". Many blog posts still show `kafka.strimzi.io/v1beta2` and the `strimzi.io/kraft: enabled` / `strimzi.io/node-pools: enabled` annotations. Those manifests are rejected by 1.x. Use `apiVersion: kafka.strimzi.io/v1`.
- Broker resources now live on node pools. The 0.48.0 entry says: "CPU and memory configuration for the Kafka nodes in `.spec.kafka.resources` is deprecated and will be removed in the `v1` CRD API. Please use the `KafkaNodePool` resources to configure CPU and memory for Kafka nodes."
- Kubernetes support:
  - The 1.2.0 deploying guide says: "You can deploy Strimzi on Kubernetes 1.30 and later" ([deploying docs](https://strimzi.io/docs/operators/latest/deploying.html)).
  - The unreleased 1.3.0 entry says: "**From Strimzi 1.3.0 on, we support only Kubernetes 1.32 and newer.**"
  - kind is at v0.33.0 ([kind releases](https://github.com/kubernetes-sigs/kind/releases/latest)), so its default node image satisfies both.
- 1.2.0 says: "The Cluster, Topic, and User Operator YAML installation files and the Cluster Operator Helm Chart now use the default container security context that matches the Restricted Kubernetes Pod Security Standard."

**Install: Helm or YAML.** Both are first-party.
- YAML quickstart ([strimzi.io/quickstarts](https://strimzi.io/quickstarts/)):
  ```bash
  kind create cluster
  kubectl create namespace kafka
  kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
  kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml -n kafka
  kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
  ```
- Helm, from the [deploying guide](https://strimzi.io/docs/operators/latest/deploying.html): "helm install strimzi-cluster-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator".
- The chart's `values.yaml` at tag 1.2.0 ([values.yaml](https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/1.2.0/helm-charts/helm3/strimzi-kafka-operator/values.yaml)) sets these defaults: `watchNamespaces: []`, `watchAnyNamespace: false`, `defaultImageTag: 1.2.0`, operator `resources: limits: memory: 384Mi, cpu: 1000m; requests: memory: 384Mi, cpu: 200m`.
- For Argo CD, use the Helm chart as an OCI source, pinned to `1.2.0`. Set `watchNamespaces: [staging, prod]` (or one `kafka` namespace that both environments share).
- CRD caveat from the Helm docs: "There is no support at this time for upgrading or deleting CRDs using Helm" ([Helm CRD best practices](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/)). Before upgrading Strimzi, apply the new CRDs separately. An Argo CD app that syncs the chart's `crds/` directory with `ServerSideApply=true` also works.
- The deploying guide warns that "each watched namespace should contain only one instance of a specific component type, such as one Kafka cluster, to avoid conflicts". So use one Kafka cluster per namespace, or one shared `kafka` namespace for both staging and prod topics with a topic prefix.

**Minimal laptop manifests.** These are adapted from the official 1.2.0 `kafka-single-node.yaml` (fetched from [strimzi.io/examples/latest](https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml)). The changes are a smaller disk, explicit resources and JVM heap, no User Operator, and one NodePort listener.
```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  labels:
    strimzi.io/cluster: fraud-kafka
spec:
  replicas: 1
  roles: [controller, broker]          # one KRaft node does both jobs
  resources:
    requests: { memory: 1Gi, cpu: 250m }
    limits:   { memory: 1536Mi }
  jvmOptions:
    "-Xms": 512m
    "-Xmx": 768m
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 10Gi                     # official example asks for 100Gi
        deleteClaim: true
        kraftMetadata: shared
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: fraud-kafka
spec:
  kafka:
    version: 4.3.1
    metadataVersion: 4.3-IV0
    listeners:
      - name: plain                    # in-cluster clients
        port: 9092
        type: internal
        tls: false
      - name: external                 # laptop clients, see kind config below
        port: 9094
        type: nodeport
        tls: false
        configuration:
          bootstrap:
            nodePort: 32100
          brokers:
            - broker: 0
              nodePort: 32000
              advertisedHost: localhost
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
      auto.create.topics.enable: false
  entityOperator:
    topicOperator:
      resources:
        requests: { memory: 256Mi, cpu: 50m }
        limits:   { memory: 384Mi }
    # userOperator omitted: there is no auth on the local cluster (Q22)
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: transactions
  labels:
    strimzi.io/cluster: fraud-kafka    # must match, or the Topic Operator ignores it
spec:
  partitions: 6                        # fixed for life: the key-to-partition map depends on it
  replicas: 1
  config:
    retention.ms: -1                   # see Q5 gotcha about event-time timestamps
    message.timestamp.type: CreateTime
```
The `KafkaTopic` label requirement is quoted from the [deploying guide](https://strimzi.io/docs/operators/latest/deploying.html): "If the label does not match the Kafka cluster, the Topic Operator cannot see the KafkaTopic, and the topic is not created." The official topic example is `apiVersion: kafka.strimzi.io/v1`, `kind: KafkaTopic`, `spec: partitions/replicas/config` ([kafka-topic.yaml @1.2.0](https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/1.2.0/packaging/examples/topic/kafka-topic.yaml)).

The NodePort override syntax and `advertisedHost` come from the [configuring guide §10.7 "Overriding assigned node ports"](https://strimzi.io/docs/operators/latest/configuring.html): "By default, the port numbers used for the bootstrap and broker services are automatically assigned by Kubernetes. You can override the assigned node ports for nodeport listeners by specifying the desired port numbers." The `advertisedHost` per-broker override is documented in the same guide. *Unverified on this machine:* `advertisedHost: localhost` combined with the kind port mappings below is the usual laptop recipe, but nobody has run it here yet.

**Memory footprint.**
- Strimzi sets no default broker resources. The configuring guide says: "If a memory limit (and request) is not specified, a JVM's minimum heap size is set to 128M. The JVM's maximum heap size is not defined to allow the memory to increase as needed. This is ideal for single node environments in test and development." It also warns: "Total JVM memory usage can be a lot more than the maximum heap size." ([configuring docs](https://strimzi.io/docs/operators/latest/configuring.html))
- Estimate, not measured, built from the limits above plus the chart default for the operator:
  - cluster operator 384Mi
  - broker/controller ~1–1.5Gi
  - topic operator ~256–384Mi
  - **total about 2–2.5 GiB**
- That fits in the ~17 GiB available alongside the other platform components (DISCOVERY facts).

**Client connectivity.**
- *Inside the cluster*, the bootstrap is `fraud-kafka-kafka-bootstrap.<ns>.svc:9092`. The quickstart test clients use `--bootstrap-server my-cluster-kafka-bootstrap:9092` ([quickstart](https://strimzi.io/quickstarts/)). The scoring service and the replay producer should both run in-cluster, so neither needs external access.
- *Outside the cluster* (laptop debugging, notebooks), a NodePort listener plus kind port mappings works. The kind docs say: "To use port mappings with NodePort, the kind node containerPort and the service nodePort needs to be equal." ([kind configuration](https://kind.sigs.k8s.io/docs/user/configuration/))
  ```yaml
  # kind-config.yaml
  kind: Cluster
  apiVersion: kind.x-k8s.io/v1alpha4
  nodes:
    - role: control-plane
      extraPortMappings:
        - { containerPort: 32100, hostPort: 32100, listenAddress: "127.0.0.1" }  # bootstrap
        - { containerPort: 32000, hostPort: 32000, listenAddress: "127.0.0.1" }  # broker 0
  ```
  Clients then use `bootstrap.servers=localhost:32100`.
- The bootstrap address is published in the Kafka status: `kubectl get kafka fraud-kafka -o=jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}'` ([deploying docs](https://strimzi.io/docs/operators/latest/deploying.html)).
- `kubectl port-forward` to the bootstrap service does **not** work reliably. Brokers advertise their own addresses, which is why `advertisedHost` exists.

**What changes on GKE** (Terraform only, never applied; ADR-0003):
- Node pools:
  - use separate `controller` (3 replicas) and `broker` (≥3 replicas) pools
  - use replication factor 3 and `min.insync.replicas: 2`
  - set `rack.topologyKey: topology.kubernetes.io/zone` for zone spreading
- Storage: use `class: standard-rwo` or `premium-rwo` on the persistent claims.
- External listener: `type: loadbalancer` or `ingress`. NodePort is a kind-only choice. In-cluster clients keep the same `*-kafka-bootstrap:9092` address, so the scoring and replay charts are unchanged.
- The Helm/Argo CD install of the operator is the same.
- The managed alternative (Google Managed Service for Apache Kafka) belongs in the build-vs-buy section, as Q26 already says.

#### Q2. Python Kafka client

| Client | Latest (date) | Python 3.13 | Nature | Notes |
|---|---|---|---|---|
| confluent-kafka | **2.15.1** (PyPI 2026-09-10; librdkafka v2.15.1, 2026-09-09) | cp313 wheels incl. `manylinux_2_28_x86_64`; also cp314 / 3.14t | C extension over librdkafka | KIP-848 GA since 2.12.0; `AIOProducer`/`AIOConsumer` non-experimental since 2.13.0 |
| aiokafka | **0.14.0** (2026-04-29) | cp313 + cp314 wheels; requires >=3.10 | asyncio, mostly pure Python | 0.15.0 (unreleased) drops 3.10 |
| kafka-python | **3.0.11** (2026-08-16) | pure-Python wheel (works on 3.13) | pure Python | Actively maintained again; 3.0.x released June to August 2026 |

Sources: PyPI JSON ([confluent-kafka](https://pypi.org/pypi/confluent-kafka/json), [aiokafka](https://pypi.org/pypi/aiokafka/json), [kafka-python](https://pypi.org/pypi/kafka-python/json)); [confluent CHANGELOG](https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md); [aiokafka CHANGES.rst](https://github.com/aio-libs/aiokafka/blob/master/CHANGES.rst); [kafka-python CHANGES.md](https://github.com/dpkp/kafka-python/blob/master/CHANGES.md); [librdkafka releases](https://github.com/confluentinc/librdkafka/releases).

Key quotes:
- confluent 2.12.0: "Starting with __confluent-kafka-python 2.12.0__, the next generation consumer group rebalance protocol defined in **KIP-848** is **production-ready**." Also: "`group.protocol` configuration property dictates whether to use the new `consumer` protocol or older `classic` protocol. It defaults to `classic` if not provided." ([CHANGELOG](https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md))
- confluent 2.13.0 (2025-12-15): "Remove experimental module designation for Async classes (#2143)" and "Expose deterministic partitioner functions (#2116)".
- `AIOConsumer` docstring: "Every method dispatches the underlying blocking Consumer call to a thread pool executor and returns an awaitable … librdkafka's Consumer is not thread-safe, so concurrent access to the same AIOConsumer instance is serialized". On `poll()` it says: "prefer consume() over poll(): consume() can retrieve multiple messages per call and amortize the async overhead across the entire batch." ([_AIOConsumer.py](https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/aio/_AIOConsumer.py))
- Performance is a vendor claim with no independent benchmark found: "Built on `librdkafka` (C library) for maximum throughput and minimal latency, significantly outperforming pure Python implementations." ([README](https://github.com/confluentinc/confluent-kafka-python/blob/master/README.md)) It does not matter here. At 1 sim day ≈ 30 s and about 2,500 Sparkov transactions per simulated day, the stream is about 85 msg/s.
- KIP-848 requirements, from the [migration guide](https://github.com/confluentinc/confluent-kafka-python/blob/master/docs/kip-848-migration-guide.md):
  - "Broker version **4.0.0+**"
  - "Rebalance callbacks (**incremental only**)"
  - "The `partitions` list passed to `incremental_assign()` and `incremental_unassign()` contains only the **incremental changes**"
  - "**Do not** use `consumer.assign()` or `consumer.unassign()` when using `group.protocol='consumer'`"
  - Strimzi 1.2.0 ships Kafka 4.3.1, so KIP-848 is available.

**Choice:** use **confluent-kafka 2.15.x**. Reasons:
- It is the only one of the three with GA KIP-848 support.
- It has `on_assign`/`on_revoke`/`on_lost` callbacks, `store_offsets`, and seek-on-assign.
- It ships wheels for Python 3.13.
- It uses the same librdkafka as kcat.

aiokafka is a reasonable second choice for a pure-asyncio style. kafka-python is maintained again, but its speed (pure Python) and the "Performance Note" in the confluent README count against it.

**Per-card ordering.** Kafka guarantees order only within one partition. Producing every Transaction with `key = card_hash` sends all of a card's events to one partition, so one consumer sees them in order. That holds as long as:
1. every producer of the topic uses the **same partitioner**;
2. the partition count never changes;
3. the producer keeps order under retries (`enable.idempotence=true`).

**Partitioner mismatch: the question's premise is wrong for librdkafka.** The default is **not** murmur2 in confluent-kafka:
- librdkafka `partitioner` defaults to `consistent_random`, documented as "CRC32 hash of key (Empty and NULL keys are randomly partitioned)". The Java-compatible option is a separate setting: "`murmur2_random` - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is functionally equivalent to the default partitioner in the Java Producer.)" ([librdkafka CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md))
- aiokafka `DefaultPartitioner`: "Hashes key to partition using murmur2 hashing (from java client)" ([aiokafka/partitioner.py](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/partitioner.py))
- kafka-python `DefaultPartitioner`: "Hashes key to partition using murmur2 hashing (from java client)" ([kafka/partitioner/default.py](https://github.com/dpkp/kafka-python/blob/master/kafka/partitioner/default.py))
- Java (Kafka 4.3): "If no partition is specified but a key is present, choose a partition based on a hash of the key." The hash is murmur2 ([producer configs](https://kafka.apache.org/43/generated/producer_config.html)).

Consumers never hash keys. They read whatever partition a record landed in, so the producer/consumer library pairing cannot cause a mismatch. The risk is **two producers** (or a producer plus code that computes partitions itself) using different hashes. Examples:
- a Python confluent producer (CRC32) and a Java `kafka-console-producer` or aiokafka tool (murmur2) both writing card-keyed records, which splits one card across partitions and breaks ordering and state ownership;
- a test that predicts partition ownership with the wrong hash.

Fix: set `"partitioner": "murmur2_random"` explicitly on every confluent producer. The project then matches Java and aiokafka/kafka-python. Never compute partition numbers in application code; store `msg.partition()` instead.

```python
from confluent_kafka import Producer
producer = Producer({
    "bootstrap.servers": "fraud-kafka-kafka-bootstrap.kafka.svc:9092",
    "partitioner": "murmur2_random",   # Java-compatible; librdkafka default is CRC32
    "enable.idempotence": True,        # librdkafka default is false
    "linger.ms": 5,
    "compression.type": "lz4",
})
```
On the librdkafka idempotence default: "`enable.idempotence` … default false … When set to `true`, the producer will ensure that messages are successfully produced exactly once and in the original produce order." ([CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md))

#### Q3. Stateful consumer: state, rebalances, offsets

**Primary-source pattern.** The Kafka `KafkaConsumer` Javadoc (4.3), section "Storing Offsets Outside Kafka" ([Javadoc](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)):
> "If the results of the consumption are being stored in a relational database, storing the offset in the database as well can allow committing both the results and offset in a single transaction. Thus either the transaction will succeed and the offset will be updated based on what was consumed or the result will not be stored and the offset won't be updated."

and
> "If the partition assignment is done automatically special care is needed to handle the case where partition assignments change. … when partitions are taken from a consumer the consumer will want to commit its offset for those partitions … When partitions are assigned to a consumer, the consumer will want to look up the offset for those new partitions and correctly initialize the consumer to that position".

This fits the project well. Predictions and card state both go to Postgres, so **one Postgres transaction per micro-batch** writes three things together:
- the prediction rows (champion and challenger),
- the dirty card states,
- the next offset per partition.

Kafka's own committed offset then serves only lag dashboards. Postgres is the source of truth. The result is **effectively exactly-once into Postgres, with no Kafka transactions**. A crash loses nothing. On restart the consumer seeks to the stored offset and recomputes the in-flight events, and the state function is pure and deterministic (ADR-0002).

**confluent-kafka API facts** (from docstrings in [Consumer.c](https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/src/Consumer.c)):
- `on_assign` is a "callback to provide handling of customized offsets on completion of a successful partition re-assignment."
- `on_revoke` is a "callback to provide handling of offset commits to a customized store on the start of a rebalance operation."
- `on_lost` handles "the case the partition assignment has been lost. If not specified, lost partition events will be delivered to on_revoke, if specified. Partitions that have been lost may already be owned by other members in the group and therefore committing offsets, for example, may fail."
- `poll()`: "Callbacks may be called from this method, such as `on_assign`, `on_revoke`". The callbacks therefore run on the polling thread, so state can be owned by that one thread without locks.

**Sketch** (sync consumer on a dedicated thread, KIP-848 incremental protocol):
```python
# scoring/stream.py
import psycopg
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from features import update_state, empty_state   # the ADR-0002 pure function

TOPIC = "transactions"

class StreamScorer:
    def __init__(self, conf, dsn, models):
        self.c = Consumer({
            **conf,
            "group.id": "scoring",
            "group.protocol": "consumer",        # KIP-848, broker >= 4.0
            "enable.auto.commit": False,         # Postgres holds the truth
            "auto.offset.reset": "earliest",
        })
        self.db = psycopg.connect(dsn)           # owned by this thread only
        self.models = models
        self.state: dict[int, dict[bytes, dict]] = {}   # partition -> card_hash -> state

    # rebalance callbacks: they run inside poll(), on this same thread
    def on_assign(self, consumer, partitions):
        with self.db.cursor() as cur:
            for tp in partitions:
                cur.execute("SELECT card_hash, state FROM card_state WHERE topic=%s AND partition=%s",
                            (TOPIC, tp.partition))
                self.state[tp.partition] = {k: v for k, v in cur}
                cur.execute("SELECT next_offset FROM consumer_offsets WHERE topic=%s AND partition=%s",
                            (TOPIC, tp.partition))
                row = cur.fetchone()
                tp.offset = row[0] if row else OFFSET_BEGINNING
        consumer.incremental_assign(partitions)  # seeks to the offsets set above

    def on_revoke(self, consumer, partitions):
        self.flush()                             # persist the pending batch while we still own it
        for tp in partitions:
            self.state.pop(tp.partition, None)
        consumer.incremental_unassign(partitions)

    def on_lost(self, consumer, partitions):
        self.pending.clear()                     # someone else may own them now: do not write
        for tp in partitions:
            self.state.pop(tp.partition, None)
        consumer.incremental_unassign(partitions)

    def run(self, stop):
        self.c.subscribe([TOPIC], on_assign=self.on_assign,
                         on_revoke=self.on_revoke, on_lost=self.on_lost)
        self.pending = []
        while not stop.is_set():
            for msg in self.c.consume(num_messages=500, timeout=0.2):
                if msg.error():
                    continue                     # log it; partition EOF etc.
                txn = decode(msg.value())
                p = msg.partition()
                s = self.state[p].get(txn.card_hash) or empty_state()
                s, x = update_state(s, txn)      # same function as the training replay
                self.state[p][txn.card_hash] = s
                self.pending.append((msg, txn, s, self.models.score(x, txn)))
            if self.pending:
                self.flush()
        self.flush(); self.c.close()

    def flush(self):
        if not self.pending:
            return
        with self.db.transaction(), self.db.cursor() as cur:
            with cur.copy("COPY predictions (txn_id, role, model_version, score, decision, applied,"
                          " event_time, kafka_partition, kafka_offset) FROM STDIN") as cp:
                for msg, txn, _, outs in self.pending:
                    for o in outs:               # champion + challenger (shadow)
                        cp.write_row((txn.id, o.role, o.version, o.score, o.decision, o.applied,
                                      txn.event_time, msg.partition(), msg.offset()))
            last = {}
            for msg, txn, s, _ in self.pending:
                last[(msg.partition(), txn.card_hash)] = s
            cur.executemany(
                "INSERT INTO card_state (topic, partition, card_hash, state) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (card_hash) DO UPDATE SET state=EXCLUDED.state, partition=EXCLUDED.partition",
                [(TOPIC, p, k, Jsonb(v)) for (p, k), v in last.items()])
            nxt = {}
            for msg, *_ in self.pending:
                nxt[msg.partition()] = msg.offset() + 1
            cur.executemany(
                "INSERT INTO consumer_offsets (topic, partition, next_offset) VALUES (%s,%s,%s) "
                "ON CONFLICT (topic, partition) DO UPDATE SET next_offset=EXCLUDED.next_offset",
                [(TOPIC, p, o) for p, o in nxt.items()])
        self.c.commit(offsets=[TopicPartition(TOPIC, p, o) for p, o in nxt.items()],
                      asynchronous=True)        # informational only (lag metrics)
        self.pending.clear()
```
Notes on the sketch:
- It is illustrative and has not been run. `decode`, `Jsonb` (`psycopg.types.json.Jsonb`) and `models.score` are placeholders.
- Loading every card of a partition into memory on assign is fine at Sparkov scale. Sparkov has about 1,000 cards, so state is a few MB.
- If the team prefers the classic eager protocol, replace `incremental_assign`/`incremental_unassign` with `assign`/`unassign` and treat the partition lists as the full assignment.

**Ordering rule if Kafka offsets were the source of truth instead** (the periodic-snapshot variant): snapshot state *first*, commit the offset *after*. Never commit an offset past the last durable snapshot. On restart, replay from the snapshot's offset.

**At-least-once vs exactly-once:**
- *At-least-once* (auto-commit, periodic snapshot): the simplest option. Restarts can produce duplicate prediction rows, which need `ON CONFLICT DO NOTHING` on `(txn_id, role)`. COPY cannot do that directly, so you would COPY into a temp table and then `INSERT … SELECT … ON CONFLICT`.
- *Kafka EOS transactions* (`transactional.id`, `send_offsets_to_transaction`) only cover outputs **written back to Kafka**. They do not cover Postgres, so they are the wrong tool here.
- *Offsets in Postgres in the same transaction* (above) gives exactly-once effects for this sink at about the same complexity as at-least-once. **Recommended.** It is also a strong interview talking point.

#### Q4. Service shape, model loading, Postgres writes

**One deployable or two.** Recommendation: **one image and one FastAPI app** containing:
- a background stream consumer **thread**, started and stopped in `lifespan`;
- `POST /score` for contract tests and k6;
- `/healthz` and `/readyz` (ready only after models are loaded and partitions assigned);
- `/metrics`.

Run it with **one uvicorn worker per pod**. In-memory card state is per process, so multiple workers would split the state.

FastAPI says the lifespan code before `yield` "will be executed **before** the application starts" and the code after "**after** the application has finished". It also warns: "If you provide a `lifespan` parameter, `startup` and `shutdown` event handlers will no longer be called." ([FastAPI lifespan events](https://fastapi.tiangolo.com/advanced/events/); FastAPI 0.142.2, PyPI 2026-09-30)

The same image can run as two Deployments (`MODE=stream` / `MODE=api`) through Helm values if the walkthrough needs a "separate microservices" story. The platform already shows several services: replay producer, scorer, Airflow, MLflow, LLM.

Important design constraint for `/score` (Q34 contract and k6 tests):
- It must **not mutate stream state** and must **not write into the monitoring tables**. Otherwise k6 traffic corrupts the velocity features and the drift statistics.
- Make `/score` a dry run: read the card's current state if this pod owns it (else `empty_state()`), call `update_state` on a copy, score, and return the decision.
- Optionally log the call with `source='http'` to a separate table.

```python
# scoring/app.py
import threading, asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.models = ModelHolder(name="fraud-lgbm")    # loads @champion and @challenger
    app.state.models.refresh()
    app.state.stop = threading.Event()
    app.state.scorer = StreamScorer(kafka_conf(), dsn(), app.state.models)
    t = threading.Thread(target=app.state.scorer.run, args=(app.state.stop,), daemon=True)
    t.start()
    poller = asyncio.create_task(app.state.models.poll_aliases(every_s=15))
    yield
    poller.cancel()
    app.state.stop.set()
    await asyncio.to_thread(t.join, 30)                  # flush and close the consumer cleanly

app = FastAPI(lifespan=lifespan)

@app.post("/score")
def score(txn: TransactionIn) -> DecisionOut:          # sync def -> runs in the threadpool
    s = app.state.scorer.peek_state(txn.card_hash)      # read-only copy
    _, x = update_state(s, txn)
    return app.state.models.score(x, txn)[0].to_out()   # champion decision only
```
Why a plain thread rather than `AIOConsumer`:
- The rebalance callbacks need sync DB I/O.
- One thread owning the consumer and the state avoids lock juggling.
- confluent's `AIOConsumer` just "dispatches the underlying blocking Consumer call to a thread pool executor" anyway ([source](https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/aio/_AIOConsumer.py)).
- LightGBM calls its C library through `ctypes.CDLL`, which releases the GIL during the call. Python's docs describe `PyDLL` as behaving "like CDLL instances, except that the Python GIL is **not** released during the function call" ([ctypes docs](https://docs.python.org/3.13/library/ctypes.html)). So scoring in the consumer thread does not starve the HTTP event loop.

**Loading champion and challenger from MLflow by alias** (MLflow 3.16.1, PyPI 2026-09-16):
- The [Model Registry docs](https://mlflow.org/docs/latest/ml/model-registry/) say: "Model aliases allow you to assign a mutable, named reference to a particular version of a registered model." They give the URI form `models:/MyModel@champion`, and say: "You can then update the model serving production traffic by reassigning the champion alias to a different model version."
- `MlflowClient.get_model_version_by_alias(name: str, alias: str) -> ModelVersion` returns "the model version instance by name and alias" ([mlflow.client API](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.client.html)).
- Hot-reload pattern (polling; MLflow OSS has no push notification):
```python
import asyncio
import mlflow, mlflow.lightgbm
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

class ModelHolder:
    def __init__(self, name):
        self.name, self.client = name, MlflowClient()
        self.slots = {}                                   # role -> (version, booster); swapped atomically

    def _resolve(self, alias):
        try:
            return self.client.get_model_version_by_alias(self.name, alias).version
        except MlflowException:
            return None                                   # e.g. no challenger right now

    def refresh(self):
        new = dict(self.slots)
        for role in ("champion", "challenger"):
            v = self._resolve(role)
            if v is None:
                new.pop(role, None)
            elif role not in new or new[role][0] != v:
                new[role] = (v, mlflow.lightgbm.load_model(f"models:/{self.name}/{v}"))
        self.slots = new                                  # one reference assignment; readers see old or new

    async def poll_aliases(self, every_s):
        while True:
            await asyncio.sleep(every_s)
            await asyncio.to_thread(self.refresh)
```
Notes on the pattern:
- Load by **resolved version number**, not `@alias`, and log `model_version` on every prediction row. If the alias moves between resolve and load, you still know exactly which model scored what.
- Shadow mode (Q16): the challenger scores every event and its row is written with `applied=false`.
- Canary at 10%: choose deterministically, for example `int.from_bytes(card_hash[:8]) % 100 < 10`, then set `applied=true` on the challenger row and `applied=false` on the champion row for those cards. Hashing by card keeps each card in one arm.

**Writing predictions to Postgres** (psycopg 3.3.6, PyPI 2026-09-18; psycopg-pool 3.3.3):
- From the [psycopg COPY docs](https://www.psycopg.org/psycopg3/docs/basic/copy.html): "COPY is one of the most efficient ways to load data into the database". `with cursor.copy("COPY sample (col1, col2, col3) FROM STDIN") as copy: for record in records: copy.write_row(record)`. The async form is `async with cursor.copy(...) as copy: await copy.write(...)`.
- From the [pipeline docs](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html), starting from Psycopg 3.1: "`executemany()` makes use internally of the pipeline mode; as a consequence there is no need to handle a pipeline block just to call `executemany()` once."
- So: COPY for the append-only `predictions` table, and `executemany` upserts for `card_state` and `consumer_offsets`, all in one transaction per micro-batch (Q3 sketch).
- Use a sync connection in the consumer thread. Use psycopg async (`AsyncConnectionPool`) only if `/score` ever writes, which the recommendation says it should not.

#### Q5. Replay producer on an accelerated clock

**Concepts** (Flink 2.3 docs, [Time concepts](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/)):
- "Event time is the time that each individual event occurred on its producing device."
- "Processing time refers to the system time of the machine that is executing the respective operation."
- "A Watermark(t) declares that event time has reached time t in that stream, meaning that there should be no more elements from the stream with a timestamp t' <= t."
- On idle partitions ([generating watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/)): "If one of the input splits/partitions/shards does not carry events for a while this means that the `WatermarkGenerator` also does not get any new information on which to base a watermark. We call this an idle input or an idle source."

**Replay pattern (anchor-and-sleep).** Fix `(sim_t0, wall_t0, speedup)`. Each event is due at `wall_t0 + (event_time - sim_t0) / speedup`. Sort by event time, sleep until each event is due, then produce. 1 sim day ≈ 30 s gives `speedup = 86400 / 30 = 2880`. Replaying 12 months takes about 3 hours of wall time.
```python
# replay/producer.py
import time, json
from datetime import datetime, timedelta

class SimClock:
    def __init__(self, sim_t0: datetime, speedup: float):
        self.sim_t0, self.speedup, self.wall_t0 = sim_t0, speedup, time.monotonic()
    def due(self, event_time: datetime) -> float:          # monotonic wall time when the event is due
        return self.wall_t0 + (event_time - self.sim_t0).total_seconds() / self.speedup
    def now(self) -> datetime:                             # current simulated time
        return self.sim_t0 + timedelta(seconds=(time.monotonic() - self.wall_t0) * self.speedup)

def replay(rows, producer, speedup=2880.0, heartbeat_every_sim_s=3600):
    clock = SimClock(rows[0].event_time, speedup)          # rows pre-sorted by event_time
    next_hb = rows[0].event_time
    for r in rows:
        delay = clock.due(r.event_time) - time.monotonic()
        if delay > 0:
            time.sleep(delay)                              # if behind, do not sleep: catch up
        producer.produce("transactions", key=r.card_hash, value=json.dumps(r.payload()),
                         headers={"event_time": r.event_time.isoformat()})
        producer.poll(0)
        if r.event_time >= next_hb:                        # clock tick for consumers with idle partitions
            producer.produce("sim-clock", value=r.event_time.isoformat())
            next_hb += timedelta(seconds=heartbeat_every_sim_s)
    producer.flush()
```
Design notes:
- Make `speedup` and `start/end` CLI flags. Keep a **max-burst** guard: if the producer falls behind, it sends immediately rather than sleeping, so lag shows up downstream instead of the clock drifting.
- Run the producer **in-cluster** as a Kubernetes Job (or Deployment with a resume offset). Persist the last produced `event_time` to Postgres so a restarted replay resumes rather than duplicating.

**How downstream components read "now":**
- *Scoring service:* "now" **is the Transaction's event time**. Features are computed as of `txn.event_time` inside the pure state function. Training replay does the same, so wall-clock time never enters the feature path (ADR-0002 parity).
- *Per-partition watermark:* use `max(event_time seen)` per assigned partition, and the minimum across partitions for anything that needs "all events up to t have arrived" (for example closing a monitoring window). Publish it as a gauge and store it in a one-row `sim_clock` table.
- *Batch jobs, label maturation, drift windows (Airflow):* read the simulated time from that `sim_clock` table, or from the `sim-clock` heartbeat topic. Never call `datetime.now()`. Windows are defined in event time, for example "PSI over sim-day D". A job is triggered when the watermark passes the window's end, not on a wall-clock cron. At 30 s per sim day, a cron would also be far too coarse.
- *Delayed labels:* the label producer uses the same clock and emits a label when `sim_now >= event_time + label_delay`.

**Kafka record timestamps: do not stamp 2019/2020 event times as `CreateTime`.** Kafka 4.3 [topic configs](https://kafka.apache.org/43/generated/topic_config.html):
- `message.timestamp.after.max.ms` defaults to "3600000 (1 hour)", and with `CreateTime` "the message will be rejected if the difference in timestamps exceeds this specified threshold".
- `message.timestamp.before.max.ms` defaults to `9223372036854775807`, so past timestamps are accepted.
- Time-based retention uses message timestamps. KIP-32 says: "The log retention will take a look at the last time index entry in the time index file. Because the last entry will be the latest timestamp in the entire log segment. If that entry expires, the log segment will be deleted." ([KIP-32](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message)) With the default `retention.ms` of "604800000 (7 days)", segments stamped 2019 would be eligible for deletion almost at once.
- KIP-32 itself recommends: "If CreateTime is required, it can always be put into the message payload."

So leave the Kafka timestamp as the produce wall time (do not pass `timestamp=`), and carry `event_time` in the payload and a header. Alternatively set the topic to `LogAppendTime` or `retention.ms: -1`.

### Recommendation

1. **Kafka:**
   - Strimzi **1.2.0** (Kafka 4.3.1, KRaft, `kafka.strimzi.io/v1`), installed by Argo CD from the OCI Helm chart `oci://quay.io/strimzi-helm/strimzi-kafka-operator`, pinned to `1.2.0`. Manage CRDs separately for upgrades.
   - One dual-role `KafkaNodePool` replica with a 768m heap and a 1.5Gi limit, the Topic Operator only, and a `transactions` topic with **6 partitions, fixed forever**, plus `labels` and `sim-clock` topics. Budget about 2–2.5 GiB.
   - All clients in-cluster via `fraud-kafka-kafka-bootstrap:9092`. A NodePort listener (`localhost:32100`) with kind `extraPortMappings` for laptop debugging only.
   - On GKE: 3 controllers + 3 brokers, RF 3, `min.insync.replicas: 2`, a `standard-rwo` storage class and zone rack-awareness. The application charts stay the same.
2. **Client:** confluent-kafka **2.15.x** for both producer and consumer.
   - Producer: `partitioner=murmur2_random`, `enable.idempotence=true`, key = hashed `cc_num`.
   - Consumer: `group.protocol=consumer` (KIP-848) and incremental rebalance callbacks.
3. **State and offsets:** pure `update_state` over a per-partition dict owned by one consumer thread. On each micro-batch (≤500 msgs or 200 ms), **one Postgres transaction** writes the prediction rows (COPY), the dirty card states (upsert) and the next offsets. `on_assign` restores state and seeks to the stored offsets. `on_revoke` flushes and then drops state. `on_lost` drops state without writing. This gives exactly-once effects into Postgres without Kafka transactions.
4. **Service:**
   - One FastAPI image. `lifespan` starts the consumer thread and an MLflow alias poller (15 s) that loads `@champion`/`@challenger` by resolved version and swaps them atomically. Run one uvicorn worker per pod and replicas ≤ partitions.
   - `POST /score` is a side-effect-free dry run for contract and k6 tests.
   - Every prediction row records `model_version`, `role`, `applied` and `event_time`.
   - Optionally split into stream and api Deployments from the same image through Helm values.
5. **Clock:**
   - The replay producer uses anchor-and-sleep at `speedup=2880`, carries event time in the payload and headers (not as the Kafka timestamp), and emits `sim-clock` heartbeats.
   - The scorer's "now" is the transaction's event time.
   - Batch and monitoring jobs read the simulated watermark from Postgres `sim_clock` and never call `datetime.now()`.

### Risks and gotchas

- **Contradicts the question's premise (Q15 / ADR-0002 ordering):** confluent-kafka's default partitioner is CRC32 (`consistent_random`), not murmur2. A mixed-tool producer set silently splits cards across partitions. Set `partitioner=murmur2_random` everywhere.
- **Refines ADR-0002's consequence "a crash loses state since the last snapshot":**
  - With state and offsets in the same Postgres transaction, nothing is lost.
  - Even with periodic snapshots, nothing is lost *if* the offset is never committed ahead of the snapshot, because Kafka retains the events and the function is deterministic.
  - The ADR's statement is only true if offsets are committed independently (for example with auto-commit). Consider updating the ADR wording.
- **Q34 conflict risk:** if `/score` mutates card state or writes to the predictions table, k6 load corrupts velocity features and drift and shadow metrics. Keep it a dry run.
- **Q10 risk:** stamping historical event time as the Kafka `CreateTime` makes retention delete data at once (7-day default against 2019 timestamps). Shifted-to-future times over 1 h are rejected. Keep event time in the payload.
- **Never change the partition count** of `transactions`. It remaps cards to partitions, and stored `card_state.partition` and offsets become wrong. Replicas above the partition count sit idle.
- **Old Strimzi tutorials** (`v1beta2`, `strimzi.io/kraft` annotations, ZooKeeper sections, `.spec.kafka.resources`) fail on 1.x. Strimzi 1.3.0 (upcoming) requires Kubernetes ≥ 1.32.
- **Helm does not upgrade CRDs.** A Strimzi chart bump without applying new CRDs leaves the operator and the CRDs out of step.
- **Strimzi docs:** "each watched namespace should contain only one instance of a specific component type, such as one Kafka cluster". Sharing one Kafka between `staging` and `prod` namespaces needs a deliberate choice: one shared `kafka` namespace with prefixed topics or consumer groups, or two small clusters (about 2× the memory).
- **KIP-848 contract:** callbacks receive *incremental* partition lists. `assign()`/`unassign()` are forbidden with `group.protocol=consumer`. Session timeouts are broker-side (`group.consumer.session.timeout.ms`). Static membership fences the *new* member on a duplicate `group.instance.id` ([migration guide](https://github.com/confluentinc/confluent-kafka-python/blob/master/docs/kip-848-migration-guide.md)).
- **Rebalance callbacks run inside `poll()/consume()`.** A slow Postgres restore in `on_assign` counts against `max.poll.interval.ms`. That is fine at Sparkov scale (about 1k cards), but measure it.
- **`on_lost`:** do not flush pending results. Another member may already own the partition, and the docstring warns that "committing offsets, for example, may fail".
- **The uvicorn `--workers N` flag** creates N consumers with N private states, and `/score` hits a random one. Use one worker per pod and scale with pods.
- **NodePort `advertisedHost: localhost`** has not been tested on this machine. If it fails, run debugging clients in-cluster (`kubectl run … quay.io/strimzi/kafka:1.2.0-kafka-4.3.1`, from the quickstart).
- **The strongest performance claims** for confluent-kafka are vendor statements. Throughput is irrelevant at about 85 msg/s anyway.
- **Unverified estimate:** the 2–2.5 GiB Kafka footprint is built from the configured limits, not measured.

### Sources

- Strimzi releases (1.2.0 on 2026-08-20; 1.1.0 on 2026-06-27; 1.0.1 on 2026-06-17): https://github.com/strimzi/strimzi-kafka-operator/releases and https://api.github.com/repos/strimzi/strimzi-kafka-operator/releases
- Strimzi CHANGELOG: https://github.com/strimzi/strimzi-kafka-operator/blob/main/CHANGELOG.md
- Strimzi quickstart (1.2.0 / Kafka 4.3.1 images): https://strimzi.io/quickstarts/
- Strimzi single-node example: https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml
- Strimzi topic example @1.2.0: https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/1.2.0/packaging/examples/topic/kafka-topic.yaml
- Strimzi deploying guide (1.2.0): https://strimzi.io/docs/operators/latest/deploying.html
- Strimzi configuring guide (node ports, JVM memory): https://strimzi.io/docs/operators/latest/configuring.html
- Strimzi Helm values @1.2.0: https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/1.2.0/helm-charts/helm3/strimzi-kafka-operator/values.yaml
- Helm CRD caveats: https://helm.sh/docs/chart_best_practices/custom_resource_definitions/
- kind configuration (extraPortMappings, NodePort): https://kind.sigs.k8s.io/docs/user/configuration/ ; kind v0.33.0: https://github.com/kubernetes-sigs/kind/releases/latest
- confluent-kafka on PyPI (2.15.1, 2026-09-10): https://pypi.org/pypi/confluent-kafka/json
- confluent-kafka CHANGELOG: https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md
- confluent-kafka README: https://github.com/confluentinc/confluent-kafka-python/blob/master/README.md
- confluent-kafka KIP-848 migration guide: https://github.com/confluentinc/confluent-kafka-python/blob/master/docs/kip-848-migration-guide.md
- confluent-kafka Consumer docstrings: https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/src/Consumer.c
- confluent-kafka AIOConsumer: https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/aio/_AIOConsumer.py
- librdkafka CONFIGURATION.md (partitioner, idempotence, offsets): https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md ; v2.15.1 on 2026-09-09: https://github.com/confluentinc/librdkafka/releases
- aiokafka on PyPI (0.14.0, 2026-04-29): https://pypi.org/pypi/aiokafka/json ; changelog: https://github.com/aio-libs/aiokafka/blob/master/CHANGES.rst ; partitioner: https://github.com/aio-libs/aiokafka/blob/master/aiokafka/partitioner.py
- kafka-python on PyPI (3.0.11, 2026-08-16): https://pypi.org/pypi/kafka-python/json ; changelog: https://github.com/dpkp/kafka-python/blob/master/CHANGES.md ; partitioner: https://github.com/dpkp/kafka-python/blob/master/kafka/partitioner/default.py
- Kafka 4.3 producer configs: https://kafka.apache.org/43/generated/producer_config.html
- Kafka 4.3 topic configs: https://kafka.apache.org/43/generated/topic_config.html
- Kafka 4.3 KafkaConsumer Javadoc ("Storing Offsets Outside Kafka"): https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
- KIP-32 (message timestamps, retention): https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
- FastAPI lifespan events (FastAPI 0.142.2): https://fastapi.tiangolo.com/advanced/events/
- MLflow Model Registry (aliases; MLflow 3.16.1): https://mlflow.org/docs/latest/ml/model-registry/
- MLflow client API (get_model_version_by_alias): https://mlflow.org/docs/latest/api_reference/python_api/mlflow.client.html
- psycopg COPY: https://www.psycopg.org/psycopg3/docs/basic/copy.html
- psycopg pipeline mode / executemany: https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html
- psycopg 3.3.6 / psycopg-pool 3.3.3 on PyPI: https://pypi.org/pypi/psycopg/json , https://pypi.org/pypi/psycopg-pool/json
- Python ctypes (GIL release by CDLL vs PyDLL): https://docs.python.org/3.13/library/ctypes.html
- Flink 2.3 time concepts: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/
- Flink idle sources: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/
- k6 latest (v2.3.0) for the Q34 load test: https://github.com/grafana/k6/releases/latest
