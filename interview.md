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

Quelles sont les responsabilités des deux consumers ?

### Réponse

Les deux consomment les événements `OrderCreated` du topic `order-events`. Payment (`payment-group`) journalise une simulation avec `orderId` et `amount`; Notification (`notification-group`) journalise une simulation avec `orderId` et `customerId`.

```text
                    order-events
                         |
                   OrderCreated
                    /         \
          payment-group    notification-group
                |                 |
       Payment Consumer    Notification Consumer
```

Ces actions locales sont intentionnelles : elles permettent d'étudier Kafka sans API de paiement ou de notification, base de données, identifiants ou dépendances externes. Aucun paiement ni envoi de notification réel n'est effectué.

### Question

Pourquoi Payment et Notification ont-ils des consumer groups différents ?

### Réponse

Un consumer group représente une identité logique de consommation. Les groupes distincts `payment-group` et `notification-group` reçoivent le même record indépendamment et gardent chacun leur propre progression; leurs cas d'usage ne se partagent donc pas le travail.

### Question

Quelle est la différence entre un consumer et un consumer group ?

### Réponse

Un consumer est un client Kafka qui lit et traite les records. Le consumer group est l'identité logique partagée par un ou plusieurs consumers, à laquelle Kafka assigne les partitions et associe les offsets.

### Question

Comment deux groupes traitent-ils indépendamment le même record ?

### Réponse

Chaque groupe lit le topic selon ses propres offsets. Le groupe Payment peut traiter l'événement pour simuler un paiement et le groupe Notification peut traiter le même événement pour simuler une notification, sans se répartir ces actions entre eux.

### Question

Deux consumer groups peuvent-ils consommer le même message Kafka ?

### Réponse

Oui. Kafka conserve le record dans le topic; chaque groupe le lit selon sa propre position. La consommation par un groupe ne supprime pas le record et ne fait pas avancer l'offset d'un autre groupe.

### Question

Que se passe-t-il si deux consumers appartiennent au même groupe plutôt qu'à des groupes différents ?

### Réponse

Dans un même groupe, les consumers se partagent les partitions; une partition est attribuée à un seul consumer actif à la fois. Des groupes distincts ont chacun leur propre lecture et peuvent donc traiter chacun le même record.

### Question

Que se passe-t-il s'il y a une partition et plusieurs consumers dans le même groupe ?

### Réponse

Un seul consumer du groupe reçoit la partition; les autres n'ont pas de partition à traiter et restent inactifs pour ce topic. Pour illustrer davantage de parallélisme au sein du groupe, il faudrait plusieurs partitions, sujet à étudier plus tard.

### Question

Quel est le lien entre topic, partition et consumer group ?

### Réponse

Le topic est composé de partitions, chacune étant un journal ordonné de records. Dans chaque groupe, Kafka assigne les partitions aux consumers et conserve séparément l'offset de progression du groupe.

### Question

Les offsets sont-ils partagés entre les groupes ? Deux offsets peuvent-ils avoir la même valeur ?

### Réponse

Chaque groupe suit et valide ses offsets indépendamment. `payment-group` et `notification-group` peuvent afficher le même nombre d'offset, mais ces positions appartiennent à des suivis distincts.

### Question

Que signifie le lag d'un consumer group ?

### Réponse

Le lag représente le retard du groupe : l'écart entre la position à laquelle il doit lire et les records disponibles à la fin de la partition. Il aide à voir si le groupe traite les messages moins vite qu'ils n'arrivent.

### Question

Pourquoi les consumer groups sont-ils utiles en architecture événementielle ?

### Réponse

Ils isolent les cas d'usage : plusieurs services peuvent réagir au même événement avec leur propre rythme et leur propre progression. Ici, Payment et Notification sont indépendants et ne nécessitent pas de coordination directe.

### Question

Comment Kafka répartit-il les partitions entre les consumers d'un groupe ? Deux consumers du même groupe peuvent-ils lire la même partition en même temps ?

### Réponse

Kafka assigne les partitions aux membres actifs du groupe; une partition donnée est assignée à un seul consumer du groupe à la fois. Le parallélisme de lecture dans le groupe est ainsi limité par le nombre de partitions.

### Question

Qu'avons-nous effectivement vérifié manuellement avec Kafka UI ?

### Réponse

Lors des vérifications précédentes, l'endpoint Kafka UI a répondu HTTP 200. La lecture de records, de leurs clés et de leurs offsets a été vérifiée avec Kafka CLI; cela ne prouve pas qu'une inspection des messages dans l'interface a été faite. Aucune vérification manuelle Kafka UI n'a été faite pour cette tâche.

---

## Questions Kafka à connaître à l'oral

1. **Qu'est-ce que Kafka ?** Une plateforme distribuée de journal d'événements : les producers écrivent dans des topics et les consumers les lisent.
2. **Qu'est-ce qu'un broker Kafka ?** Un serveur Kafka qui stocke les partitions et sert les lectures et écritures des clients.
3. **Qu'est-ce qu'un topic ?** Un flux logique de records Kafka, réparti en partitions.
4. **Qu'est-ce qu'une partition et pourquoi en utiliser ?** Un journal ordonné de records; plusieurs partitions permettent de répartir stockage et traitement.
5. **Qu'est-ce qu'un offset ?** La position d'un record dans une partition; il est suivi séparément pour chaque consumer group.
6. **Qu'est-ce qu'un consumer ?** Un client qui lit les records d'un topic et exécute un traitement.
7. **Qu'est-ce qu'un consumer group ?** Une identité logique de consommation; ses membres se partagent les partitions et le groupe maintient ses positions de lecture.
8. **Même groupe ou groupes différents ?** Dans un groupe, les consumers se partagent les partitions; des groupes distincts lisent indépendamment le même topic.
9. **Deux groupes peuvent-ils lire le même record ?** Oui. Kafka garde un suivi de lecture propre à chaque groupe; la lecture par l'un n'empêche pas l'autre de lire le record.
10. **Comment les partitions sont-elles réparties dans un groupe ?** Kafka assigne chaque partition à un seul consumer actif du groupe; le nombre de consumers utiles en parallèle est limité par le nombre de partitions.
11. **Deux consumers d'un groupe peuvent-ils lire la même partition simultanément ?** Non, une partition est attribuée à un seul consumer du groupe à la fois.
12. **Que se passe-t-il avec une partition et plusieurs consumers dans le groupe ?** Un seul reçoit la partition; les autres restent inactifs pour cette partition. Plusieurs partitions et le parallélisme sont des sujets à étudier plus tard.
13. **Que se passe-t-il si un consumer tombe ?** Kafka rééquilibre le groupe et réassigne ses partitions aux membres restants; la reprise utilise les offsets du groupe.
14. **Qu'est-ce qu'un offset ? Est-il partagé entre groupes ?** Il repère une position dans une partition. Chaque groupe suit ses propres offsets; deux groupes peuvent avoir la même valeur numérique sans partager leur progression.
15. **Que signifie le lag ?** L'écart entre la position de lecture du groupe et les records disponibles à la fin de la partition; il indique le retard de consommation.
16. **Quel est le lien topic, partition et consumer group ?** Un topic contient des partitions ordonnées; un groupe se voit attribuer ces partitions par consumer et suit séparément ses offsets.
17. **Pourquoi Payment et Notification ont-ils des groupes différents ?** `payment-group` et `notification-group` reçoivent chacun le même record pour leurs responsabilités indépendantes.
18. **Pourquoi les actions sont-elles simulées ?** Pour apprendre Kafka sans dépendre d'API externes, d'identifiants ou d'une base; aucune intégration réelle n'est présente.
19. **Producer et consumer : quelle différence ?** Le producer publie les records; le consumer les lit et déclenche leur traitement.
20. **Que fait `KafkaTemplate` ?** C'est l'API Spring Kafka utilisée par l'application pour envoyer des records au broker.
21. **Que fait `@KafkaListener` ?** Il relie une méthode Spring à un topic et à un consumer group afin que Spring Kafka lui transmette les records.
22. **Comment le Payment Consumer reçoit-il `OrderCreated` ?** Le listener consomme `order-events`; le `JsonDeserializer` Spring Kafka transforme le payload JSON en `OrderCreated`.
23. **Pourquoi l'API attend-elle avant `201 Created` ?** Elle attend la confirmation du producer Kafka afin de ne pas annoncer une publication réussie avant l'accusé de réception du broker.
24. **Que se passe-t-il si Kafka est indisponible lors de la publication ?** L'API renvoie `503 Service Unavailable`; elle ne retourne pas de succès 2xx quand la publication n'est pas confirmée.
25. **« Envoi demandé » signifie-t-il « publication confirmée » ?** Non. `KafkaTemplate.send` lance l'envoi et retourne un future; seule la complétion réussie de ce future confirme l'acceptation Kafka.
26. **Que vérifions-nous avec Kafka UI ?** Les vérifications précédentes ont obtenu HTTP 200 sur l'endpoint Kafka UI; la lecture des records, clés et offsets a été vérifiée par CLI. L'interface n'a pas été utilisée manuellement pour cette tâche.
27. **Qu'est-ce que KRaft ? Quelle différence avec ZooKeeper ?** KRaft gère les métadonnées Kafka avec le mécanisme intégré à Kafka; l'ancien mode s'appuyait sur ZooKeeper, absent de ce POC.
28. **Pourquoi éviter la logique métier complexe dans le listener ?** Un listener devrait surtout adapter et déléguer le message; séparer le traitement facilite les tests, la lisibilité et l'évolution.

---

## Complete Kafka POC Flow

### Déroulement

1. Docker Compose démarre le broker Kafka local en mode KRaft; son healthcheck permet de vérifier qu'il répond.
2. `POST /orders` reçoit `customerId` et `amount`. L'API crée `orderId` et `createdAt`, puis publie l'événement JSON `OrderCreated` dans `order-events`, avec `orderId` comme clé.
3. L'API attend la confirmation de publication Kafka avant de répondre `201 Created`; si l'envoi échoue ou n'est pas confirmé, elle retourne `503`, pas un succès 2xx.
4. `payment-group` consomme le record et journalise la simulation de paiement. `notification-group` reçoit indépendamment le même record et journalise la simulation de notification.
5. Chaque groupe suit ses propres offsets. On peut inspecter le record, sa clé, sa partition et son offset avec Kafka CLI ou Kafka UI; la CLI `kafka-consumer-groups --describe` expose les positions et le lag.

Le topic a actuellement une seule partition. Le parcours démontre les consumer groups indépendants, pas le parallélisme entre plusieurs partitions.

### Concepts démontrés

**Implémentés et exercés dans ce POC :** producer, consumers, topic, partition unique, clé de message, sérialisation JSON, confirmation de publication Kafka, réponse HTTP en cas d'échec, consumer groups distincts, offsets et lag, KRaft et broker Kafka local sous Docker. Le test d'intégration de bout en bout utilise le broker local et vérifie la réponse HTTP, le record Kafka, les deux actions de consommation et la progression indépendante des groupes.

**Observations manuelles antérieures :** des commandes Kafka CLI ont vérifié le broker, le topic et des records. Kafka UI a répondu sur son endpoint lors d'une vérification précédente, mais cela ne prouve pas qu'un humain ait parcouru les messages dans l'interface. La vérification manuelle du parcours complet et de Kafka UI reste à faire.

**À étudier plus tard :** plusieurs partitions et leur parallélisme, ainsi que toute fonctionnalité Kafka avancée; elles ne sont pas implémentées par ce POC.

### Questions courtes pour l'entretien

1. **Expliquez le flux complet du POC.** L'API construit `OrderCreated`, le publie dans `order-events` et attend l'accusé Kafka avant `201`. Payment et Notification lisent chacun cet événement avec leur propre groupe.
2. **Pourquoi attendre la confirmation Kafka avant `201` ?** Pour ne pas annoncer une création réussie avant que Kafka confirme la publication.
3. **Que se passe-t-il si Kafka est indisponible ?** La publication échoue ou n'est pas confirmée; l'API retourne `503 Service Unavailable` plutôt qu'un succès.
4. **Pourquoi deux consumer groups ?** Chaque cas d'usage reçoit le même événement et avance indépendamment, au lieu de se partager le travail.
5. **Pourquoi deux groupes peuvent-ils lire le même événement ?** Kafka conserve le record dans le topic et suit une progression distincte pour chaque groupe.
6. **Que représente un offset ?** Une position dans une partition; le consumer group sauvegarde sa position de lecture pour reprendre ultérieurement.
7. **Pourquoi les groupes peuvent-ils afficher la même valeur d'offset ?** La valeur numérique peut coïncider, mais chaque offset appartient au suivi indépendant de son groupe.
8. **Qu'est-ce que le lag ?** L'écart entre l'offset courant du groupe et la fin de la partition; il indique les records qui restent à lire.
9. **Quel est le rôle de la clé `orderId` ?** Elle sert à identifier le record et à déterminer sa partition; elle ne déduplique pas les publications.
10. **Quel est le rôle d'une partition ?** C'est un journal ordonné de records. Le POC en utilise une; l'étude du parallélisme multi-partitions est différée.
11. **Producer et consumer : quelle différence ?** Le producer écrit des records dans un topic; les consumers les lisent et exécutent leur traitement.
12. **Qu'est-ce que KRaft et pourquoi pas ZooKeeper ?** KRaft gère les métadonnées avec le mécanisme intégré à Kafka; le POC local évite le service ZooKeeper.

---

## Fiabilité du producer et du consumer : ce que le POC fait réellement (tâche 1.1)

> Cette section décrit l'état **actuel** du POC, vérifié par `KafkaEffectiveDefaultsTest`. Rien n'est encore configuré explicitement : la tâche 1.2 le fera.

### Valeurs effectives découvertes (producer, client Kafka 3.9.1)

| Paramètre | Valeur effective | Source |
|---|---|---|
| `acks` | `all` (stocké `-1` par le client) | défaut du client Kafka |
| `enable.idempotence` | `true` | défaut du client Kafka |
| `retries` | `2147483647` | défaut du client Kafka |
| `delivery.timeout.ms` | `120000` (2 min) | défaut du client Kafka |
| `max.block.ms` | `60000` (1 min) | défaut du client Kafka |

Aucune de ces clés n'est dans `application.properties` : ce sont des **défauts**, pas une configuration choisie par le POC.

### Consumer (Payment et Notification)

- Commit d'offset : `enable.auto.commit=false` au runtime, mode d'acquittement Spring `BATCH` : l'offset est validé **après** que le listener a traité les records du `poll()`.
- Gestion d'erreur : aucun error handler configuré, donc le `DefaultErrorHandler` de Spring Kafka s'applique : **10 livraisons au total** (9 retries) sans délai, puis le record est seulement journalisé et **ignoré**. Pas de DLT.
- `auto.offset.reset=latest` par défaut (un nouveau groupe ne lit pas l'historique).

### Rappel théorique : à quoi servent ces paramètres

- **`acks`** : combien d'accusés le broker doit donner avant de confirmer (`0` aucun, `1` le leader, `all` tous les replicas in-sync). Plus c'est strict, plus la durabilité augmente et plus la latence aussi.
- **`enable.idempotence`** : le broker écarte les doublons dus aux retries du producer (par partition, pendant la vie du producer). Exige `acks=all`.
- **`retries`** : nombre de réenvois après une erreur transitoire. En pratique, c'est `delivery.timeout.ms` qui borne le temps total.
- **`delivery.timeout.ms`** : durée maximale entre l'envoi (`send`) et le résultat final, succès ou échec, retries compris.
- **`max.block.ms`** : durée maximale pendant laquelle `send()` peut bloquer (métadonnées indisponibles, buffer plein) avant de lever une exception.

### Propriété Spring Boot vs configuration effective du client

Une propriété `spring.kafka.producer.*` n'est qu'une **entrée** transmise au client Kafka. Si elle est absente, le client applique son défaut interne, et c'est ce défaut qui compte. Il faut donc lire la configuration **effective** (log `ProducerConfig values`, ou `new ProducerConfig(...)`) plutôt que supposer. Piège observé : `ConsumerFactory.isAutoCommit()` renvoie `true` (défaut brut du client) alors que le conteneur Spring démarre les vrais consumers avec `enable.auto.commit=false`.

### Comportement actuel vs théorie vs à venir

| | Contenu |
|---|---|
| **Comportement actuel du POC** | Producer déjà `acks=all` + idempotent par défaut ; `send().get()` sans borne explicite (jusqu'à environ 60 s à 120 s) ; consumers en at-least-once ; échec Payment : 10 tentatives puis perte silencieuse. |
| **Théorie Kafka** | `acks=0/1/all`, replicas, ISR, `min.insync.replicas` : un seul broker ici, donc leurs effets de durabilité ne sont pas démontrables. |
| **Tâche 1.2 (pas encore faite)** | Rendre ces valeurs explicites dans la configuration et borner l'attente HTTP. Ce n'est **pas** encore configuré. |

### Questions d'entretien dérivées

1. **Quelle est la valeur par défaut de `acks` ?** Avec un client Kafka récent (3.x), `all`, et l'idempotence est activée par défaut. Il faut vérifier la version du client, car les anciens clients utilisaient `1`.
2. **Que contrôle `acks` ?** Le niveau d'accusé exigé du broker avant de considérer l'envoi réussi : `0` aucun, `1` leader seul, `all` tous les replicas in-sync.
3. **À quoi sert l'idempotence du producer ?** À éviter les doublons créés par les retries du producer vers une même partition. Elle ne protège pas contre un doublon créé par l'application elle-même (deux appels distincts).
4. **`retries` ou `delivery.timeout.ms` : lequel limite vraiment ?** Le second : les retries continuent jusqu'à ce délai total écoulé.
5. **Que se passe-t-il si Kafka est arrêté pendant `send()` ?** `send()` peut bloquer jusqu'à `max.block.ms` (métadonnées), puis l'envoi échoue au plus tard après `delivery.timeout.ms`. Dans le POC, l'API retourne alors `503`, mais sans borne explicite l'attente reste longue.
6. **Quand le consumer valide-t-il son offset ici ?** Après le traitement du listener (mode `BATCH`, auto-commit désactivé). Un crash avant le commit provoque un retraitement : c'est de l'at-least-once.
7. **Que fait Spring Kafka quand un listener échoue, sans configuration ?** Il réessaie jusqu'à 10 livraisons sans délai, puis journalise et saute le record ; il n'y a pas de DLT, donc le message est perdu pour ce groupe.
8. **Pourquoi lire la configuration effective ?** Une propriété Spring absente n'est pas « non configurée » : le client applique un défaut, et seul le log ou un test montre la valeur réellement utilisée.

---

## Producer reliability rendue explicite (tâche 1.2)

> Contrairement à la tâche 1.1 (valeurs par défaut du client), ces valeurs sont maintenant **configurées** dans `application.properties` et vérifiées par `KafkaEffectiveDefaultsTest`.

### Valeurs configurées et pourquoi

| Paramètre | Valeur | Rôle |
|---|---|---|
| `acks` | `all` | Le leader attend tous les replicas in-sync (ici, un seul broker). |
| `enable.idempotence` | `true` | Écarte les doublons causés par les retries du producer. |
| `retries` | `2147483647` | Quasi illimité : c'est le délai total qui borne. |
| `delivery.timeout.ms` | `5000` | Durée maximale entre `send()` et le résultat final, retries compris. |
| `request.timeout.ms` | `3000` | Attente d'une réponse du broker pour une requête. |
| `max.block.ms` | `2000` | Durée maximale pendant laquelle `send()` bloque (métadonnées, buffer). |
| `app.orders.publication-timeout` | `6s` | Attente HTTP de la confirmation Kafka (propriété de l'application, pas du client Kafka). |

Contrainte Kafka : `delivery.timeout.ms >= request.timeout.ms + linger.ms`. Sans baisser `request.timeout.ms` (défaut 30 s), `delivery.timeout.ms=5000` serait refusé au démarrage du producer.

### Les trois étapes d'une publication

1. **Envoyé au producer** : `send()` rend la main, le record est dans le buffer. Rien n'est garanti.
2. **Confirmé par Kafka** : le `Future` se termine avec succès après l'accusé du broker.
3. **Réponse HTTP** : `201` seulement après l'étape 2.

### Comportement en cas de timeout

- Si Kafka ne confirme pas dans les 6 s, `get(timeout)` lève `TimeoutException`, transformée en `OrderPublicationException` puis en `503`.
- Le temps d'attente maximal d'une requête est d'environ `max.block.ms` + attente du `Future` (environ 8 s), au lieu de 1 à 3 minutes avec les défauts.
- Après un timeout, le résultat est **inconnu** : le record peut quand même arriver dans Kafka. Le client peut donc réessayer et créer un doublon métier. L'idempotence du producer ne l'empêche pas (deux commandes distinctes ont deux `orderId`).

### Questions d'entretien dérivées

1. **Pourquoi `delivery.timeout.ms` doit-il être supérieur à `request.timeout.ms` ?** Il couvre l'envoi, les retries et les attentes ; Kafka exige `delivery.timeout.ms >= request.timeout.ms + linger.ms`, sinon le producer refuse de démarrer.
2. **Pourquoi borner l'attente HTTP ?** Sans borne, une panne Kafka bloquerait les threads HTTP pendant des minutes ; on préfère échouer vite avec `503`.
3. **Pourquoi 6 s alors que `delivery.timeout.ms` vaut 5 s ?** Pour laisser le producer annoncer son propre échec définitif ; la borne HTTP n'est qu'un filet de sécurité.
4. **Que veut dire un timeout sur `get()` ?** Que la confirmation n'est pas arrivée, pas que l'envoi a échoué : l'état est incertain.
5. **L'idempotence du producer évite-t-elle un doublon après un retry du client HTTP ?** Non : elle ne protège que les retries internes du producer ; un nouvel appel `POST /orders` crée un nouvel `orderId`.
6. **Quelle différence entre `max.block.ms` et `delivery.timeout.ms` ?** Le premier borne le blocage de l'appel `send()` ; le second borne le temps entre l'envoi et le résultat final.

*La théorie complète (`acks=0/1/all`, réplication, ISR, `min.insync.replicas`) viendra avec la tâche 1.3.*

---

## Étapes de publication et théorie `acks` / réplication (tâche 1.3)

> Ce qui est **réellement configuré et observé** dans le POC : 1 broker, 1 partition, replication factor 1, `acks=all`, idempotence activée, `delivery.timeout.ms=5000`, `app.orders.publication-timeout=6s`, `201` après confirmation, `503` sinon. Tout le reste de cette section (réplicas, ISR, `min.insync.replicas`, `acks=0/1`) est de la **théorie non démontrée** avec un seul broker.

### Les trois étapes

1. **Accepté par le producer** : `send()` a rendu un future, le record est dans le buffer ; aucune garantie.
2. **Confirmé par Kafka** : le future se termine avec succès après l'accusé du broker.
3. **Réponse HTTP** : `201` seulement après l'étape 2 ; `503` si la confirmation n'arrive pas dans les 6 s (ou en cas d'échec ou d'interruption).

Si Kafka confirme mais que la réponse `201` est perdue, le message est publié alors que le client croit à un échec ; s'il réessaie, une nouvelle commande (nouvel `orderId`) est créée. Ce n'est **pas** une transaction HTTP/Kafka.

### Théorie (non démontrée ici)

- `acks=0` : aucun accusé attendu, risque de perte silencieuse. `acks=1` : le leader accuse après écriture locale, perte possible s'il tombe avant la copie. `acks=all` : le leader attend les replicas in-sync requises.
- **Leader** : réplica qui traite les écritures d'une partition ; **réplicas** : copies qui suivent le leader.
- **ISR** : réplicas suffisamment à jour (leader inclus). **`min.insync.replicas`** : taille minimale de l'ISR pour accepter une écriture `acks=all`. Exemple : RF=3, `min.insync.replicas=2`, ISR tombée à 1 : écriture refusée.

### Ne pas confondre

`acks` = niveau d'accusé ; idempotence = pas de doublon dû aux retries du producer ; `retries` = tentatives ; `delivery.timeout.ms` = limite globale de livraison d'un record ; `app.orders.publication-timeout` = attente de notre requête HTTP (propriété applicative).

### Questions d'entretien

1. **Que signifie `acks=all` ?** Le leader attend l'accusé des réplicas in-sync requises avant de confirmer. C'est le niveau d'attente de réplication le plus fort des trois.
2. **`acks=1` vs `acks=all` ?** Avec `1`, seul le leader accuse : un crash du leader avant réplication perd le record. Avec `all`, les réplicas in-sync doivent l'avoir reçu.
3. **Qu'est-ce qu'un leader ? Une réplica ?** Le leader gère les écritures d'une partition ; les réplicas en sont des copies qui suivent le leader et peuvent le remplacer.
4. **Qu'est-ce que l'ISR ?** L'ensemble des réplicas suffisamment à jour avec le leader ; `acks=all` s'évalue sur cet ensemble.
5. **À quoi sert `min.insync.replicas` ?** À refuser une écriture `acks=all` quand l'ISR est trop petite, plutôt que de réduire silencieusement la durabilité.
6. **`acks=all` garantit-il seul qu'un message n'est jamais perdu ?** Non. Avec un ISR réduit au leader et `min.insync.replicas=1`, `all` n'apporte rien de plus ; il faut aussi RF > 1 et un `min.insync.replicas` adapté.
7. **Idempotence producer vs idempotence métier ?** La première évite les doublons dus aux retries du producer vers une partition ; la seconde évite de traiter deux fois le même ordre métier côté consumer ou API.
8. **Confirmation Kafka vs réponse HTTP ?** La confirmation est l'accusé du broker ; la réponse HTTP est ce que le client reçoit. La seconde peut échouer après la première.
9. **Kafka confirme mais la réponse HTTP est perdue ?** Le message est publié, le client croit à un échec. Un retry crée un doublon métier ; une clé d'idempotence côté client serait nécessaire (hors scope).
10. **Pourquoi le POC ne démontre-t-il pas la réplication ?** Un seul broker, une partition, RF=1 : pas de réplica ni d'ISR à observer, donc `acks=1` et `acks=all` se comportent pareil.

---

## Retry consumer, DLT et messages malformés : décisions (tâche 2.1)

> **Décisions uniquement.** Rien n'est encore implémenté : aujourd'hui, un échec Payment subit le comportement par défaut de Spring (10 livraisons sans délai, puis le record est ignoré, sans DLT). L'implémentation viendra en 2.2 et 2.3.

### Décisions retenues (à implémenter)

- **Déclencheur d'échec** : la propriété `app.payment.fail-customer-id` ; un `customerId` égal à cette valeur fait échouer Payment (simulation, pas une logique de paiement).
- **Retry consumer** : `FixedBackOff` de 1 s, 3 retries (4 livraisons, environ 3 s), pour le consumer Payment uniquement.
- **DLT** : `order-events.DLT`, 1 partition, replication factor 1, observé mais jamais retraité.
- **Message malformé** : hors implémentation, expliqué en théorie.

### Questions d'entretien

1. **Retry producer ou retry consumer ?** Le retry producer renvoie un record vers le broker après une erreur d'envoi (`retries`, `delivery.timeout.ms`). Le retry consumer rejoue le traitement d'un record déjà lu après un échec du listener. Les deux sont indépendants.
2. **Pourquoi réessayer côté consumer ?** Parce que beaucoup d'erreurs sont transitoires (service indisponible, verrou, réseau) : un nouvel essai peut réussir sans intervention.
3. **Pourquoi un DLT ?** Pour ne pas perdre un record qui échoue toujours et ne pas bloquer la partition : il est mis de côté avec son contexte (topic, partition, offset, exception) pour analyse ou reprise.
4. **Retry ou DLT ?** Le retry traite une erreur supposée temporaire ; le DLT reçoit le record quand les retries sont épuisés. Le DLT n'est pas rejoué automatiquement.
5. **Que coûte un retry bloquant ?** Pendant les tentatives, la partition ne progresse pas : les records suivants attendent. C'est le compromis du `DefaultErrorHandler`, par rapport à des topics de retry non bloquants (hors scope).
6. **Pourquoi traiter les messages malformés à part ?** L'erreur survient à la désérialisation, avant l'appel du listener : un retry n'aide pas, car le même payload échouera toujours (poison pill). Le remède usuel est un `ErrorHandlingDeserializer` avec envoi au DLT ; il n'est pas implémenté ici, et son comportement actuel dans le POC n'a pas été vérifié.

---

## Déclencheur d'échec Payment simulé (tâche 2.2)

> Implémenté : le **déclencheur** uniquement. Les retries consumer et le DLT ne sont pas encore configurés (tâche 2.3) : un échec Payment subit encore le comportement par défaut de Spring (10 livraisons, puis le record est ignoré).

- **Mécanisme** : `app.payment.fail-customer-id` (vide par défaut). Si le `customerId` de l'événement lui est égal, `PaymentConsumer` lève une `IllegalStateException` ; sinon le paiement simulé s'exécute normalement. Une valeur vide ne fait jamais échouer.
- **Pourquoi une simulation** : pour provoquer à volonté une erreur de traitement et pouvoir observer les retries et le DLT ; ce n'est pas de la logique de paiement. `OrderCreated` et `POST /orders` ne changent pas.
- **Déterministe** : l'échec se reproduit à chaque livraison du même événement, ce qui permet d'épuiser les retries. Un échec aléatoire ne le permettrait pas.
- **Exemple** : démarrer l'application avec `--app.payment.fail-customer-id=customer-fail` puis créer une commande avec ce `customerId` ; Notification, lui, traite toujours l'événement.

### Questions d'entretien

1. **Comment faire échouer un consumer pour tester la gestion d'erreur ?** Avec un déclencheur de simulation explicite et déterministe (propriété), plutôt qu'un échec aléatoire ou un changement du contrat de l'événement.
2. **Pourquoi une exception du listener est-elle importante ?** C'est elle qui déclenche le error handler de Spring Kafka ; sans exception, le record est considéré comme traité et l'offset avance.
3. **Pourquoi le déclencheur est-il vide par défaut ?** Pour que le comportement normal du POC reste inchangé ; la panne n'existe que lorsqu'on l'active volontairement.

## Tâches 2.3 / 2.4 / 2.5 : retry consommateur, DLT et indépendance des groupes

**Q : Retry producer vs retry consumer ?**
R : Le `retries` du producer renvoie un envoi vers Kafka qui a échoué (publication). Le retry consumer (`DefaultErrorHandler` + `FixedBackOff`) rejoue le traitement d'un record déjà lu. Deux mécanismes, deux configurations, aucun lien.

**Q : Quelle configuration de retry avons-nous ?**
R : `FixedBackOff(1000, 3)` : 1 tentative initiale + 3 retries, 1 s entre chaque, donc 4 tentatives en environ 3 s, uniquement pour Payment.

**Q : Retry vs DLT ?**
R : Le retry sert aux pannes temporaires. Le DLT reçoit le record quand les retries sont épuisés, pour ne pas bloquer la partition indéfiniment et garder la trace du message en échec.

**Q : Que contient le DLT ?**
R : Le même key et le même payload que l'original, plus des headers `kafka_dlt-original-topic`, `-original-partition`, `-original-offset`, `-exception-message`. Chez nous : `order-events.DLT`, 1 partition, RF 1, jamais retraité.

**Q : Piège rencontré ?**
R : Spring Kafka 3.3 nomme le DLT par défaut `<topic>-dlt`. Notre topic déclaré s'appelle `order-events.DLT`, donc la destination est fixée explicitement. Le test a échoué tant que ce n'était pas le cas : le record partait dans un topic auto-créé.

**Q : Le retry est-il bloquant ?**
R : Oui. Pendant les 3 s, la partition n'avance pas pour Payment. Acceptable pour un POC ; des retry topics non bloquants existent mais sont hors périmètre.

**Q : Pourquoi Notification n'est-elle pas ralentie ?**
R : Chaque consumer group a ses propres offsets et son propre container. Le test montre que Notification traite l'ordre en échec et le suivant avant la 4e tentative de Payment.

**Q : Est-ce de l'exactly-once ?**
R : Non. C'est de l'at-least-once : un record peut être traité jusqu'à 4 fois et Payment n'est pas encore idempotent.

**Q : Pourquoi un `groupId` unique dans le test ?**
R : Pour lire le DLT sans perturber les offsets des groupes applicatifs. Les groupes `payment-group` et `notification-group` restent fixes (ils sont vérifiés par le test E2E), donc les deux classes de tests partagent un seul contexte Spring.