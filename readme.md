# Example Golang CQRS application

This application is using [Watermill CQRS](http://watermill.io/docs/cqrs) component.

It's an improved [6-cqrs-ordered-events](https://github.com/ThreeDotsLabs/watermill/tree/v1.4.6/_examples/basic/6-cqrs-ordered-events) example.

Events are stored in Kafka, projections are stored in PostgreSQL.

We can reset our projections and then restore their state from Kafka by resetting offsets.

See [It's Okay To Store Data In Kafka](https://www.confluent.io/blog/okay-store-data-apache-kafka/).

# Start
```bash
docker compose up -d
go run . serve
```

# Play with
```bash
# create a chat
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' --url 'http://localhost:8080/chat' -d '{"title": "new chat"}'

# see chats
curl -Ss -X GET -H 'X-UserId: 1' --url 'http://localhost:8080/chat/search' | jq

# pin chat
curl -i -X PUT -H 'X-UserId: 1' --url 'http://localhost:8080/chat/1/pin?pin=true'

# create a message
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' --url 'http://localhost:8080/chat/1/message' -d '{"content": "new message"}'
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' --url 'http://localhost:8080/chat/1/message' -d '{"content": "new message 2"}'
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' --url 'http://localhost:8080/chat/1/message' -d '{"content": "new message 3"}'

# see messages
curl -Ss -X GET --url 'http://localhost:8080/chat/1/message/search' | jq

# read message
curl -i -X PUT -H 'X-UserId: 1' --url 'http://localhost:8080/chat/1/message/2/read'

# add participant into chat
curl -i -X PUT -H 'Content-Type: application/json' --url 'http://localhost:8080/chat/1/participant' -d '{"participantIds": [2, 3]}'

# remove participant from chat
curl -i -X DELETE -H 'Content-Type: application/json' --url 'http://localhost:8080/chat/1/participant' -d '{"participantIds": [3]}'

# see participants
curl -Ss -X GET --url 'http://localhost:8080/chat/1/participants' | jq

# get his chats - see unreads
curl -Ss -X GET -H 'X-UserId: 2' --url 'http://localhost:8080/chat/search' | jq

# remove message from chat
curl -i -X DELETE  -H 'X-UserId: 1' --url 'http://localhost:8080/chat/1/message/1'

# reset offsets for consumer groups
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --group CommonProjection --reset-offsets --to-earliest --execute --topic events
# reset db
docker rm -f postgresql
docker volume rm go-cqrs-example_postgres_data
docker compose up -d postgresql
# ... or via
go run . reset
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

docker compose exec -it kafka bash
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --list
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group CommonProjection --offsets

# see kafka topic
docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic events --from-beginning --property print.key=true --property print.headers=true
```
