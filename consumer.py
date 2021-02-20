import pulsar, time
from pulsar import InitialPosition, ConsumerType

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe('public/default/test', 'my-subscription',
                            receiver_queue_size=10,
                            initial_position=InitialPosition.Earliest,
                            consumer_type=ConsumerType.Shared)

while True:
    msg = consumer.receive()
    try:
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
        time.sleep(.1)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()
