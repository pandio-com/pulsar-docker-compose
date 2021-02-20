import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer('public/default/test')

for i in range(10):
    producer.send(('Hello-%d' % i).encode('utf-8'))

client.close()