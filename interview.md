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

---

## 14. Producer, publication et contrat HTTP

### Question

Comment `POST /orders` publie-t-il un événement Kafka ?

### Réponse

Le controller reçoit `customerId` et `amount`. Le service construit un `OrderCreated` avec un `orderId` généré et `createdAt` en UTC au format ISO-8601. `KafkaTemplate` est le client Spring Kafka utilisé par le producer pour envoyer l'objet; le serializer JSON le transforme en payload JSON.

L'événement est publié sur `order-events`, avec `orderId` comme record key. Kafka range le record dans une partition; son offset identifie sa position dans cette partition. Dans ce POC, le topic a une partition et un facteur de réplication de 1.

### Question

Quelle différence entre demander un envoi et confirmer la publication ?

### Réponse

Appeler `KafkaTemplate.send` demande l'envoi et retourne un future; cela ne confirme pas encore que Kafka a accepté le record. Le service attend la fin de ce future. L'API retourne `201 Created` seulement quand la publication est confirmée.

Si Kafka est indisponible, si l'envoi échoue ou si l'attente est interrompue, la publication n'est pas confirmée : l'API répond `503 Service Unavailable`, jamais un succès 2xx. Aucun retry ni DLT n'est ajouté dans cette étape. L'ack Kafka ne signifie pas qu'un consumer a traité le message.

### Question

Comment envoyer et vérifier un événement ?

### Réponse

Exemple PowerShell :

```powershell
curl.exe -i -X POST "http://localhost:8080/orders" `
  -H "Content-Type: application/json" `
  --data-raw '{"customerId":"customer-001","amount":24.50}'
```

La réponse `201` ressemble à ceci (les valeurs générées varient) :

```json
{
  "orderId": "b1d02f1a-d3d2-4a85-8e6e-69202ef3340c",
  "customerId": "customer-001",
  "amount": 24.50,
  "createdAt": "2026-10-06T12:30:00Z"
}
```

Pour lire les records Kafka, clés et offsets :

```powershell
docker compose -f compose.yaml exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic order-events `
  --from-beginning `
  --property print.key=true `
  --property print.partition=true `
  --property print.offset=true
```

Exemple de ligne affichée (l'offset et l'identifiant sont attribués par Kafka et l'API) :

```text
Partition:0    Offset:4    b1d02f1a-d3d2-4a85-8e6e-69202ef3340c    {"orderId":"b1d02f1a-d3d2-4a85-8e6e-69202ef3340c","customerId":"customer-001","amount":24.50,"createdAt":"2026-10-06T12:30:00Z"}
```

On peut aussi ouvrir Kafka UI à <http://localhost:8081> et consulter les messages du topic.

### Question

Pourquoi deux requêtes `POST` créent-elles deux records et deux offsets ?

### Réponse

Chaque requête réussie crée un nouvel événement avec un `orderId` distinct, puis le producer envoie un nouveau record. Kafka attribue à chaque record une position dans la partition; les deux offsets sont donc différents et augmentent au fil des écritures. La record key ne déduplique pas les messages.

---

## 15. Consumer Payment

### Question

Qu'est-ce qu'un consumer Kafka et que fait `@KafkaListener` ?

### Réponse

Un consumer lit les records d'un topic Kafka. Dans Spring Kafka, `@KafkaListener` déclare une méthode appelée quand un record correspondant est reçu; ici, elle écoute `order-events` et reçoit un `OrderCreated` désérialisé depuis le JSON.

### Question

Pourquoi le Payment Consumer utilise-t-il `payment-group` ?

### Réponse

Un consumer group est l'identité de consommation dont Kafka suit les offsets et à laquelle il assigne les partitions. Le Payment Consumer utilise son propre groupe `payment-group` afin de recevoir les événements indépendamment des autres traitements futurs, comme Notification.

### Question

Quel est le lien entre topic, partition, consumer et consumer group ?

### Réponse

Un topic est un flux composé d'une ou plusieurs partitions. Dans un même groupe, Kafka répartit les partitions entre les consumers actifs; une partition n'est attribuée qu'à un seul consumer de ce groupe à la fois. Des groupes différents lisent le même topic indépendamment et suivent chacun leur progression.

### Question

Que fait le Payment Consumer de l'événement ?

### Réponse

Il journalise une simulation de paiement avec `orderId` et `amount`. Ce n'est ni un paiement réel ni une intégration métier. Le listener reste simple; une logique métier plus complexe devrait être déléguée à un service pour faciliter tests et séparation des responsabilités.

### Question

Comment Kafka suit-il la progression du groupe ?

### Réponse

Chaque record a un offset dans sa partition. Kafka stocke les offsets validés par consumer group; ils permettent au groupe de reprendre sa lecture après le dernier offset enregistré.

---

## 16. Consumer Notification

### Question

Pourquoi le Notification Consumer utilise-t-il `notification-group` ?

### Réponse

Ce consumer écoute `order-events` dans le groupe `notification-group`, différent de `payment-group`. Kafka livre ainsi le même record à chacun des groupes et suit leur progression indépendamment.

### Question

Quelle est la différence entre un consumer et un consumer group ?

### Réponse

Un consumer est un client Kafka qui lit et traite les records. Le consumer group est l'identité logique partagée par un ou plusieurs consumers, à laquelle Kafka assigne les partitions et associe les offsets.

### Question

Comment deux groupes traitent-ils indépendamment le même record ?

### Réponse

Chaque groupe lit le topic selon ses propres offsets. Le groupe Payment peut traiter l'événement pour simuler un paiement et le groupe Notification peut traiter le même événement pour simuler une notification, sans se répartir ces actions entre eux.

### Question

Que se passe-t-il si deux consumers appartiennent au même groupe plutôt qu'à des groupes différents ?

### Réponse

Dans un même groupe, les consumers se partagent les partitions; une partition est attribuée à un seul consumer actif à la fois. Des groupes distincts ont chacun leur propre lecture et peuvent donc traiter chacun le même record.

### Question

Quel est le lien entre topic, partition et consumer group ?

### Réponse

Le topic est composé de partitions, chacune étant un journal ordonné de records. Dans chaque groupe, Kafka assigne les partitions aux consumers et conserve séparément l'offset de progression du groupe.

### Question

Pourquoi les consumer groups sont-ils utiles en architecture événementielle ?

### Réponse

Ils isolent les cas d'usage : plusieurs services peuvent réagir au même événement avec leur propre rythme et leur propre progression. Ici, Payment et Notification sont indépendants et ne nécessitent pas de coordination directe.

Le Notification Consumer de ce POC ne réalise aucun envoi externe : il journalise une simulation avec `orderId` et `customerId`.

---

## Questions Kafka à connaître à l'oral

1. **Qu'est-ce que Kafka ?** Une plateforme distribuée de journal d'événements : les producers écrivent dans des topics et les consumers les lisent.
2. **Qu'est-ce qu'un broker Kafka ?** Un serveur Kafka qui stocke les partitions et sert les lectures et écritures des clients.
3. **Qu'est-ce qu'un topic ?** Un flux logique de records Kafka, réparti en partitions.
4. **Qu'est-ce qu'une partition et pourquoi en utiliser ?** Un journal ordonné de records; plusieurs partitions permettent de répartir stockage et traitement.
5. **Qu'est-ce qu'un offset ?** La position d'un record dans une partition; il est suivi séparément pour chaque consumer group.
6. **Qu'est-ce qu'un consumer ?** Un client qui lit les records d'un topic et exécute un traitement.
7. **Qu'est-ce qu'un consumer group ?** Un ensemble logique de consumers partageant la lecture des partitions et leurs offsets.
8. **Même groupe ou groupes différents ?** Dans un même groupe, les consumers se partagent les partitions; des groupes différents reçoivent chacun le flux indépendamment.
9. **Comment les partitions sont-elles réparties dans un groupe ?** Kafka assigne chaque partition à un seul consumer actif du groupe; le nombre de consumers utiles en parallèle est limité par le nombre de partitions.
10. **Que se passe-t-il si un consumer tombe ?** Kafka réassigne ses partitions aux consumers restants du groupe; le groupe reprend selon ses offsets validés.
11. **À quoi sert la clé d'un record ?** Elle participe au choix de partition; une clé stable permet de diriger les records liés vers la même partition.
12. **Comment préserver l'ordre ?** Kafka garantit l'ordre à l'intérieur d'une partition. Des records partageant une clé sont normalement dirigés vers la même partition; il n'y a pas d'ordre global entre partitions.
13. **Producer et consumer : quelle différence ?** Le producer publie des records; le consumer les lit et les traite.
14. **Que fait `KafkaTemplate` ?** C'est l'API Spring Kafka utilisée par l'application pour envoyer des records au broker.
15. **Que fait `@KafkaListener` ?** Il relie une méthode Spring à un topic et à un consumer group afin que Spring Kafka lui transmette les records.
16. **Comment le Payment Consumer reçoit-il `OrderCreated` ?** Le listener consomme `order-events`; le `JsonDeserializer` Spring Kafka transforme le payload JSON en `OrderCreated`.
17. **Pourquoi `payment-group` et `notification-group` sont-ils distincts ?** Chaque groupe reçoit le même événement et avance avec ses propres offsets; leurs traitements ne se partagent donc pas les messages.
18. **Les offsets sont-ils partagés entre consumer groups ?** Non. Chaque groupe suit sa progression séparément, ce qui permet à un nouveau cas d'usage de lire indépendamment le topic.
19. **Pourquoi l'API attend-elle avant `201 Created` ?** Elle attend la confirmation du producer Kafka afin de ne pas annoncer une publication réussie avant l'accusé de réception du broker.
20. **Que se passe-t-il si Kafka est indisponible lors de la publication ?** L'API renvoie `503 Service Unavailable`; elle ne retourne pas de succès 2xx quand la publication n'est pas confirmée.
21. **« Envoi demandé » signifie-t-il « publication confirmée » ?** Non. `KafkaTemplate.send` lance l'envoi et retourne un future; seule la complétion réussie de ce future confirme l'acceptation Kafka.
22. **Comment vérifier un record ?** Utiliser Kafka UI pour consulter le topic, ou Kafka CLI, par exemple `kafka-console-consumer` avec `--property print.key=true --property print.partition=true --property print.offset=true`.
23. **Qu'est-ce que KRaft ? Quelle différence avec ZooKeeper ?** KRaft gère les métadonnées Kafka avec le mécanisme intégré à Kafka; l'ancien mode s'appuyait sur ZooKeeper, absent de ce POC.
24. **Que représentent les actions Payment et Notification ?** Des simulations journalisées avec les champs utiles de l'événement; aucun paiement ni envoi externe n'est exécuté.
25. **Pourquoi éviter la logique métier complexe dans le listener ?** Un listener devrait surtout adapter et déléguer le message; séparer le traitement facilite les tests, la lisibilité et l'évolution.