import time
import pulsar
# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://192.168.2.49:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('DEtopic', subscription_name='DE-sub')

data = ''
while True:

    # Display message received from producer
    msg = consumer.receive()
    try:
        # Data streamin processin
        word = msg.data().decode('utf-8')
        capWord = word.upper()
        data = data + " " + capWord
        print("Received message : '%s'" % capWord)
        print("Merged message : '%s'" % data)
        consumer.acknowledge(msg)
    except Exception as e:
        print('Error: ' +str(e))
        consumer.negative_acknowledge(msg)

# Destroy pulsar clien
client.close()

