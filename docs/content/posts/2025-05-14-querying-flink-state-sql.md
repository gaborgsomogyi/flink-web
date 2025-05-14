---
title: "Querying the Inside of Your Flink Applications with State SQL"
date: "2025-05-14T00:00:00.000Z"
authors:
- gaborgsomogyi:
  name: "Gabor Somogyi"
  aliases:
  - /news/2025/05/12/querying-flink-state-sql.html
---

## Introduction: A New Era of Observability with the State Processor API

Apache Flink’s power lies in its stateful stream processing capabilities. But historically, understanding the state behind your application logic-especially after the job is running-was a challenge. Developers would often resort to instrumentation, logging, or custom code to inspect job state, debug behaviors, or validate correctness.

The [**State Processor API**](https://nightlies.apache.org/flink/flink-docs-master/docs/libs/state_processor_api/) made a major step forward by allowing users to **read and write Flink savepoints and checkpoints offline**. This enables validating state consistency, migrating applications, or implementing debugging logic without modifying running jobs.

Now, with the Flink **2.1 release**, developers can go even further-using [**SQL to query Flink state**](https://nightlies.apache.org/flink/flink-docs-master/docs/libs/state_processor_api/#table-api) directly from savepoints or checkpoints. This builds on top of the State Processor API, adding a familiar and powerful interface for state exploration. In this post, we’ll walk you through the challenges we faced, what’s newly available, and how you can start using it today to build more transparent and trustworthy Flink applications.

## The Challenges: Performance and Accessibility

### Performance: Making Large State Accessible

While the State Processor API opened the door to accessing state snapshots, it wasn’t originally optimized for scale. Processing large savepoints could be painfully slow due to deserialization bottlenecks and suboptimal I/O patterns.

Part of these issues was addressed in **[FLINK-37109](https://issues.apache.org/jira/browse/FLINK-37109)**, which delivered significant performance improvements. As demonstrated in the corresponding pull request, execution times dropped by **up to 90%**, thanks to more efficient key handling.

### Accessibility: Opening State with SQL

The second-and perhaps more transformative-challenge was usability. Even with improved performance, the Java-based State Processor API required custom code and Flink internals knowledge. This made state access impractical for quick debugging or exploratory analysis.

To solve this, Flink 2.1 introduced a **SQL connector for metadata and keyed state data**, powered by a set of FLIPs:

- [FLIP-474: Store operator name and UID in state metadata](https://cwiki.apache.org/confluence/display/FLINK/FLIP-474%3A+Store+operator+name+and+UID+in+state+metadata)
- [FLIP-496: SQL connector for keyed savepoint data](https://cwiki.apache.org/confluence/display/FLINK/FLIP-496%3A+SQL+connector+for+keyed+savepoint+data)
- [FLIP-512: Add meta information to SQL state connector](https://cwiki.apache.org/confluence/display/FLINK/FLIP-512%3A+Add+meta+information+to+SQL+state+connector)
- [FLIP-522: Add generalized type information to SQL state connector](https://cwiki.apache.org/confluence/display/FLINK/FLIP-522%3A+Add+generalized+type+information+to+SQL+state+connector)

These improvements make it possible to query and explore state snapshots using SQL-even without knowledge of internal Flink representation.

## Newly Added APIs: Peeking into State with SQL

Using the SQL state connector, you can now read snapshot data (from checkpoints or savepoints) via SQL queries. The new APIs expose both **metadata** and **user data** stored in keyed state.

Here’s a teaser how to read state data:

```sql
CREATE TABLE state_table (
  k INTEGER,
  MyValueState INTEGER,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'connector' = 'savepoint',
  'state.path' = '/checkpoint-data/chk-1',
  'operator.uid' = 'my-operator-uid',
);

SELECT * FROM state_table;
```

To analyze metadata (e.g., state size, operator UID/name, type):

```sql
LOAD MODULE state;
SELECT * FROM savepoint_metadata('/checkpoint-data/chk-1');
```

## Examples: Improving Observability with SQL and the State Processor API

### 1. Building a Session Tracking Application

Imagine a Flink job that tracks web sessions by user ID. The job stores the **start** and **end** timestamps of each session using a keyed stateful function.

```java
DataStream<WebEvent> events = ...

events
  .keyBy(WebEvent::getUserId)
  .process(new SessionTracker());

public class SessionTracker extends KeyedProcessFunction<String, WebEvent, SessionUpdate> {
  private ValueState<Long> sessionStart;
  private ValueState<Long> sessionEnd;

  @Override
  public void open(Configuration parameters) {
    sessionStart = getRuntimeContext().getState(...);
    sessionEnd = getRuntimeContext().getState(...);
  }

  @Override
  public void processElement(WebEvent event, Context ctx, Collector<SessionUpdate> out) {
    // Buggy sessionization logic here
  }
}
```

### 2. Inspecting State Metadata

```sql
LOAD MODULE state;
SELECT `operator-name`, `operator-uid`, `operator-total-size-in-bytes` FROM savepoint_metadata('/checkpoint-data/chk-1');
```

Thanks to [FLIP-474](https://cwiki.apache.org/confluence/display/FLINK/FLIP-474%3A+Store+operator+name+and+UID+in+state+metadata), Flink now embeds **human-readable** UIDs and operator names in state metadata.

### 3. Comparing Checkpoints via SQL

```sql
CREATE TABLE state1_table (
  k STRING,
  session_end BIGINT,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'connector' = 'savepoint',
  'state.path' = '/checkpoint-data/chk-1',
  'operator.uid' = 'session-operator-uid',
);
CREATE TABLE state2_table (
  k STRING,
  session_end BIGINT,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'connector' = 'savepoint',
  'state.path' = '/checkpoint-data/chk-2',
  'operator.uid' = 'session-operator-uid',
);

SELECT COUNT(*) FROM state1_table WHERE session_end IS NULL;
SELECT COUNT(*) FROM state2_table WHERE session_end IS NULL;
```

## Further Development Possibilities

- **State Processor API Source API V2 Migration**
- **Operator State Support**
- **CLI support**

## Conclusion

With the new **State SQL** features introduced in Flink 2.1, developers finally have a powerful, accessible way to explore Flink state outside the runtime context. Built on top of the [State Processor API](https://nightlies.apache.org/flink/flink-docs-master/docs/libs/state_processor_api/), this functionality bridges a long-standing gap in stateful stream processing: visibility.
