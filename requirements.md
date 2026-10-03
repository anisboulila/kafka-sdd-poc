# Requirements --- Kafka SDD POC

## 1. Objectif

Ce POC a pour objectif d'apprendre et de pratiquer les fondamentaux
d'Apache Kafka dans un contexte Java / Spring Boot, avec une approche
**Specification-Driven Development (SDD)** et **OpenSpec**.

Le POC doit permettre de comprendre les concepts Kafka suffisamment bien
pour pouvoir les expliquer et les illustrer lors d'un entretien
technique.

L'objectif principal est l'apprentissage, et non la construction d'une
application métier complète.

## 2. Scénario fonctionnel

Le POC représente un scénario simple de création de commande.

Lorsqu'une commande est créée :

``` text
Order API
   |
   | OrderCreated
   v
Kafka topic: order-events
   |              |
   v              v
Payment       Notification
Consumer       Consumer
```

L'Order Service produit un événement `OrderCreated`.

Deux consommateurs indépendants consomment cet événement :

-   **Payment Consumer** : simule le traitement du paiement.
-   **Notification Consumer** : simule l'envoi d'une notification.

Le scénario doit rester volontairement simple afin de concentrer
l'apprentissage sur Kafka.

## 3. Événement

L'événement `OrderCreated` contient au minimum :

``` text
orderId
customerId
amount
createdAt
```

L'événement sera sérialisé en JSON.

## 4. Concepts Kafka à pratiquer

Le POC doit permettre de pratiquer et de comprendre :

-   Producer
-   Consumer
-   Topic
-   Partition
-   Offset
-   Consumer Group
-   Message Key
-   JSON serialization
-   Ordering
-   Consumer parallelism
-   Replication
-   Producer acknowledgements (`acks`)
-   Producer retries
-   Producer idempotence
-   Retry côté consumer
-   Dead Letter Topic (DLT)
-   Delivery semantics
-   Idempotent Consumer
-   Kafka CLI
-   KRaft
-   Event-driven architecture

## 5. Infrastructure

Kafka sera exécuté avec Docker Compose.

Le POC utilisera **Kafka en mode KRaft**, sans ZooKeeper.

L'environnement cible sera local afin de conserver une installation et
une utilisation simples.

## 6. Technologies

Le projet utilisera principalement :

-   Java 17
-   Spring Boot 3.x
-   Spring Kafka
-   Maven
-   Apache Kafka
-   Docker
-   Docker Compose
-   Git
-   GitHub Copilot
-   OpenSpec

## 7. Approche de développement

Le développement suivra une approche **Specification-Driven
Development**.

Le flux général sera :

``` text
Requirements
    ↓
Specification
    ↓
OpenSpec Change
    ↓
Tasks
    ↓
Implementation
    ↓
Tests / Verification
    ↓
Documentation
    ↓
Git Commit
```

La spécification doit guider l'implémentation.

GitHub Copilot sera utilisé comme assistant de développement, mais les
choix d'architecture et les décisions techniques seront compris et
validés avant leur implémentation.

OpenSpec sera utilisé pour gérer les changements et les artefacts liés à
l'approche SDD.

## 8. Simplicité

Le POC doit rester petit et pédagogique.

Ne pas introduire au début :

-   Kubernetes
-   Terraform
-   Cloud
-   Base de données
-   Authentification
-   Frontend
-   CI/CD complexe
-   Monitoring avancé
-   Architecture microservices complexe

Tout ajout doit avoir une justification pédagogique claire.

## 9. Hors scope initial

Les sujets suivants pourront éventuellement être étudiés comme bonus,
mais ne doivent pas bloquer le parcours principal :

-   Outbox Pattern
-   Saga
-   Kafka Transactions avancées
-   Exactly-Once avancé
-   Schema Registry
-   Avro
-   Kafka Streams
-   Kafka Connect
-   Debezium
-   Kubernetes
-   Kafka Cloud

## 10. Résultat attendu

À la fin du POC, je dois être capable de :

1.  expliquer le fonctionnement général de Kafka ;
2.  expliquer la différence entre topic, partition et offset ;
3.  expliquer le fonctionnement d'un consumer group ;
4.  expliquer comment le message key influence le partitionnement et
    l'ordre ;
5.  expliquer comment plusieurs consumers se répartissent les partitions
    ;
6.  expliquer les principaux mécanismes de fiabilité d'un producer ;
7.  expliquer les retries et les DLT ;
8.  expliquer les principales delivery semantics ;
9.  expliquer pourquoi un consumer doit pouvoir être idempotent ;
10. utiliser les commandes Kafka CLI de base ;
11. expliquer pourquoi KRaft remplace ZooKeeper ;
12. présenter l'architecture du POC et justifier les choix techniques.

## 11. Contraintes pédagogiques

Le parcours doit privilégier :

**Compréhension \> quantité de code**

Pour chaque étape :

-   comprendre le concept ;
-   l'implémenter simplement ;
-   tester son comportement ;
-   observer le résultat ;
-   être capable de l'expliquer oralement ;
-   mettre à jour `interview.md` ;
-   effectuer un commit Git.

Une tâche ne doit pas être considérée comme terminée uniquement parce
que le code compile.
