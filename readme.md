# Example Golang CQRS application

This application is using [Watermill CQRS](http://watermill.io/docs/cqrs) component.

It's an improved [6-cqrs-ordered-events](https://github.com/ThreeDotsLabs/watermill/tree/v1.4.6/_examples/basic/6-cqrs-ordered-events) example.

Events are stored in Kafka, projections are stored in PostgreSQL.

We can reset our projections and then restore their state from Kafka by resetting offsets.

See [It's Okay To Store Data In Kafka](https://www.confluent.io/blog/okay-store-data-apache-kafka/).

# Start
```bash
docker compose up -d
go run .
```

# Play with
```bash
# create a subscriber
curl -i -X POST --url 'http://localhost:8080/subscribe'

# see subscribers
curl -Ss -X GET --url 'http://localhost:8080/subscribers' | jq

# update the subscriber
curl -i -X PUT --url 'http://localhost:8080/update/216681f5-e73e-4461-926e-019445b9913b'

# unsubscribe
curl -i -X POST --url 'http://localhost:8080/unsubscribe/216681f5-e73e-4461-926e-019445b9913b'

# see activities
curl -Ss -X GET --url 'http://localhost:8080/activities' | jq

# clear projections
docker compose exec -it postgresql psql -U postgres -c 'truncate subscriber; truncate activity_timeline;'

# now there are zeroes
curl -Ss -X GET --url 'http://localhost:8080/subscribers' | jq
curl -Ss -X GET --url 'http://localhost:8080/activities' | jq

# stop app

# reset offsets for consumer groups
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --group SubscriberProjection --reset-offsets --to-earliest --execute --topic events
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --group ActivityTimelineProjection --reset-offsets --to-earliest --execute --topic events

# start app again
go run .
# and it will restore tables from the zeroth offset, see
curl -Ss -X GET --url 'http://localhost:8080/subscribers' | jq
curl -Ss -X GET --url 'http://localhost:8080/activities' | jq
```

# Tracing
See `Trace-Id` header and put its value into [Jaeger UI](http://localhost:16686)

# Various commands
```bash
# see logs
docker compose logs -f kafka
docker compose logs -f postgresql

# see projections
docker compose exec -it postgresql psql -U postgres
select * from subscriber order by created_timestamp;
select * from activity_timeline order by created_timestamp;

docker compose exec -it kafka bash
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --list
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group SubscriberProjection --offsets
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group ActivityTimelineProjection --offsets

# see kafka topic
docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic events --from-beginning --property print.key=true --property print.headers=true
```
