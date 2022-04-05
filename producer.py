import pulsar
# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
#client = pulsar.Client('pulsar://192.168.2.208:6650')

# Create a producer on the topic that consumer can subscribe to
producer = client.create_producer('DEtopic')

data = 'I want to be capatilized'

# Send a message to consumer
for word in data.split() :
	print(word)
	producer.send((word).encode('utf-8'))
# Destroy pulsar client
client.close()

