## Purpose

Defines how the `order-events` topic is split into partitions, how events are placed by key, how consumers of one group share partitions, and what can be observed when group membership changes.

## ADDED Requirements

### Requirement: Multi-partition order topic
The system MUST support `order-events` with more than one partition while keeping `orderId` as the message key and keeping the `OrderCreated` JSON contract unchanged.

#### Scenario: Events with the same key use the same partition
- **WHEN** several events with the same `orderId` key are published
- **THEN** they MUST all be written to the same partition of `order-events`

#### Scenario: Both consumer groups still receive every event
- **WHEN** an event is published to any partition of `order-events`
- **THEN** `payment-group` and `notification-group` MUST each receive it independently

### Requirement: Per-partition ordering only
The documentation and any ordering check MUST state that Kafka guarantees order within a partition and not across the whole topic.

#### Scenario: Order within a partition
- **WHEN** events with the same key are published in sequence
- **THEN** a consumer MUST observe them in publication order for that key

#### Scenario: No global order
- **WHEN** events with different keys land on different partitions
- **THEN** the documentation MUST state that their relative order is not guaranteed

### Requirement: Partition sharing within a consumer group
Consumers in the same consumer group MUST share the partitions of a topic so that each partition is assigned to at most one consumer of the group at a time, and consumers beyond the partition count MUST receive no partition.

#### Scenario: One partition and two consumers
- **WHEN** two consumers of the same group subscribe to a one-partition topic
- **THEN** exactly one consumer MUST be assigned the partition and the other MUST be idle

#### Scenario: Two partitions and two consumers
- **WHEN** two consumers of the same group subscribe to a two-partition topic
- **THEN** each consumer MUST be assigned one distinct partition and the two MAY process events in parallel

### Requirement: Group isolation is preserved
Payment and Notification MUST remain in separate consumer groups, and adding consumers to one group MUST NOT change what the other group receives.

#### Scenario: Scaling Payment does not affect Notification
- **WHEN** a second Payment consumer joins `payment-group`
- **THEN** `notification-group` MUST keep receiving every event and its assignment MUST NOT change

### Requirement: Observable rebalance
The system MUST allow an observer to see consumer group membership and partition assignment before and after a consumer joins or leaves a group.

#### Scenario: Consumer joins
- **WHEN** an additional consumer joins a group on a multi-partition topic
- **THEN** the partitions MUST be redistributed and the new assignment MUST be observable through logs or Kafka tooling

#### Scenario: Consumer leaves
- **WHEN** a consumer leaves the group
- **THEN** its partitions MUST be reassigned to the remaining consumer and the new assignment MUST be observable
