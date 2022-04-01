import time
import pulsar
# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://192.168.2.49:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('DEtopic', subscription_name='DE-sub')

data = ''

timeout = 5
lastMsg_time = time.time()

while time.time() < lastMsg_time + timeout:
# Display message received from producer
        msg = consumer.receive()
        try:
                print("Received message : '%s'" % msg.data())
                # Acknowledge for receiving the message
                consumer.acknowledge(msg)

                # Data streamin processin
                word = msg.data()
                capWord = word.upper()
                data = data +  '' + capWord
                lastMsg_time = time.time()
        except:
            consumer.negative_acknowledge(msg)


print("Final message : '%s'" % data)

# Destroy pulsar clien
client.close()
