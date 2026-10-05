# Interview Kafka — POC SDD

## 1. Architecture du POC

### Question

Quel est l'objectif de ce POC Kafka ?

### Réponse

L'objectif est d'apprendre et de pratiquer les fondamentaux d'Apache Kafka dans un contexte Java / Spring Boot.

Le scénario est volontairement simple :

```text
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

L'objectif est de comprendre les concepts Kafka et de pouvoir les expliquer en entretien, plutôt que de construire une application métier complète.

---

## 2. Qu'est-ce qu'un Broker Kafka ?

### Question

Qu'est-ce qu'un broker Kafka ?

### Réponse

Un broker est un serveur Kafka.

Il reçoit les messages des producers, stocke les messages dans les partitions et les fournit aux consumers.

Un cluster Kafka peut contenir plusieurs brokers :

```text
Kafka Cluster

Broker 1
Broker 2
Broker 3
```

Dans notre POC, nous utilisons volontairement un seul broker pour simplifier l'apprentissage.

---

## 3. Qu'est-ce qu'un Controller Kafka ?

### Question

Quel est le rôle du controller Kafka ?

### Réponse

Le controller est responsable de la coordination du cluster Kafka et de la gestion de certaines métadonnées du cluster, notamment les informations liées aux partitions et à leur leadership.

Dans les anciennes architectures Kafka, cette fonction de coordination reposait sur ZooKeeper.

Avec KRaft, Kafka intègre directement cette gestion dans Kafka, sans ZooKeeper.

---

## 4. Broker vs Controller

### Question

Quelle est la différence entre un broker et un controller ?

### Réponse

**Broker :**

* reçoit les messages ;
* stocke les messages ;
* sert les messages aux consumers ;
* gère les partitions dont il est responsable.

**Controller :**

* coordonne le cluster ;
* gère les métadonnées du cluster ;
* participe notamment à la gestion du leadership des partitions.

Dans notre environnement local, un même nœud joue les deux rôles :

```text
Kafka Node 1
├── Broker
└── Controller
```

Configuration :

```yaml
KAFKA_PROCESS_ROLES: "broker,controller"
```

---

## 5. Qu'est-ce que KRaft ?

### Question

Pourquoi utilisons-nous KRaft ?

### Réponse

KRaft est le mode d'architecture de Kafka permettant de gérer les métadonnées et le rôle de controller directement avec Kafka, sans dépendre de ZooKeeper.

Notre POC utilise donc :

```text
Kafka + KRaft
```

et non :

```text
Kafka + ZooKeeper
```

Cela permet de pratiquer une architecture Kafka moderne tout en gardant une infrastructure locale simple.

---

## 6. Les listeners Kafka

### Question

Pourquoi avons-nous les ports 9092 et 9093 ?

### Réponse

Notre configuration contient :

```yaml
KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
```

Le port **9092** est utilisé pour les communications avec les clients Kafka, par exemple notre future application Spring Boot.

Le port **9093** est utilisé pour les communications liées au controller KRaft.

```text
9092 → Clients Kafka ↔ Broker

9093 → Controller KRaft
```

---

## 7. LISTENERS vs ADVERTISED_LISTENERS

### Question

Quelle est la différence entre `KAFKA_LISTENERS` et `KAFKA_ADVERTISED_LISTENERS` ?

### Réponse

`KAFKA_LISTENERS` indique **sur quelles adresses/ports Kafka écoute**.

Dans notre configuration :

```yaml
KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
```

`KAFKA_ADVERTISED_LISTENERS` indique l'adresse que Kafka communique aux clients pour qu'ils puissent ensuite se connecter au broker.

Dans notre POC :

```yaml
KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
```

Notre application Java pourra donc utiliser :

```text
localhost:9092
```

---

## 8. Pourquoi localhost:9092 ne fonctionne pas dans un navigateur ?

### Question

Pourquoi ne peut-on pas ouvrir `http://localhost:9092` comme une page web ?

### Réponse

Le port `9092` n'est pas un serveur HTTP.

Il utilise le protocole Kafka.

Les clients attendus sont par exemple :

* Kafka CLI ;
* une application Java ;
* Spring Kafka ;
* un autre client Kafka.

Un navigateur envoie des requêtes HTTP, que Kafka ne comprend pas.

Pour vérifier Kafka, nous utilisons par exemple :

```powershell
docker compose -f compose.yaml exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 9. Pourquoi un seul broker ?

### Question

Pourquoi avons-nous seulement un broker dans le POC ?

### Réponse

Pour commencer simplement et nous concentrer sur les fondamentaux Kafka.

Notre configuration est :

```text
1 broker
1 controller
1 partition
```

Le même nœud joue actuellement les rôles de broker et de controller.

Plus tard, nous augmenterons la complexité pour étudier :

* plusieurs partitions ;
* plusieurs consumers ;
* consumer groups ;
* parallélisme ;
* plusieurs brokers ;
* réplication ;
* leader / follower ;
* tolérance aux pannes.

---

## 10. Pourquoi une seule partition pour commencer ?

### Question

Pourquoi `KAFKA_NUM_PARTITIONS` vaut `1` ?

### Réponse

Nous commençons avec une seule partition afin de simplifier l'apprentissage.

```yaml
KAFKA_NUM_PARTITIONS: "1"
```

Cela définit le nombre de partitions par défaut lors de la création des nouveaux topics.

Cela ne signifie pas que Kafka est limité à une seule partition.

Plus tard, notre topic `order-events` pourra être configuré avec plusieurs partitions afin d'étudier le parallélisme et les consumer groups.

---

## 11. Healthcheck Kafka

### Question

Comment vérifions-nous que Kafka est réellement opérationnel ?

### Réponse

Le `compose.yaml` contient un healthcheck qui exécute :

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

Docker indique alors :

```text
Up ... (healthy)
```

Dans notre POC, nous avons obtenu :

```text
kafka-sdd-poc-kafka-1
Up 3 minutes (healthy)
```

Cela confirme que le broker répond correctement aux vérifications effectuées par le healthcheck.

---

## 12. Démarrer, vérifier et arrêter Kafka avec Docker Compose

Depuis la racine du projet, démarrer le broker et attendre qu'il soit healthy :

```powershell
docker compose -f compose.yaml up -d --wait kafka
docker compose -f compose.yaml ps
```

La colonne `STATUS` doit afficher `Up ... (healthy)`. Pour consulter les derniers logs :

```powershell
docker compose -f compose.yaml logs --tail=50 kafka
```

Pour arrêter le broker sans supprimer le conteneur :

```powershell
docker compose -f compose.yaml stop kafka
```

La commande `up -d --wait kafka` permet de le redémarrer et d'attendre à nouveau son healthcheck.

---

## 13. Vérifier Kafka avec la CLI

Les commandes Kafka CLI sont exécutées dans le conteneur. `--bootstrap-server localhost:9092` indique au client CLI comment joindre le broker :

```powershell
docker compose -f compose.yaml exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

Cette commande interroge le broker et affiche les versions des API Kafka qu'il prend en charge.

Lister les topics existants :

```powershell
docker compose -f compose.yaml exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Une liste vide signifie qu'aucun topic n'existe encore ; la commande a néanmoins vérifié la connexion si elle se termine sans erreur.

En mode KRaft, consulter l'état du quorum de métadonnées :

```powershell
docker compose -f compose.yaml exec kafka kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status
```

L'état affiche notamment l'identifiant du cluster et le controller leader. Cette commande permet d'observer le quorum KRaft sans utiliser ZooKeeper.

---

# À retenir pour l'entretien

```text
Broker
→ serveur Kafka qui stocke et sert les messages

Controller
→ coordination et métadonnées du cluster

KRaft
→ architecture Kafka sans ZooKeeper

9092
→ clients Kafka / broker

9093
→ controller KRaft

1 broker
→ simplification de notre environnement local

1 partition
→ point de départ pédagogique

advertised.listeners
→ adresse communiquée aux clients pour se connecter au broker
```

## Questions à savoir répondre oralement

1. Qu'est-ce qu'un broker Kafka ?
2. Quel est le rôle du controller ?
3. Quelle différence entre broker et controller ?
4. Qu'est-ce que KRaft ?
5. Quelle différence entre Kafka avec ZooKeeper et Kafka avec KRaft ?
6. Pourquoi avons-nous deux listeners, 9092 et 9093 ?
7. Quelle différence entre `listeners` et `advertised.listeners` ?
8. Pourquoi `localhost:9092` n'est-il pas une URL HTTP ?
9. Pourquoi commençons-nous avec un seul broker ?
10. Pourquoi commençons-nous avec une seule partition ?
