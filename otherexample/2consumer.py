import time
import pulsar
# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('DEtopic', subscription_name='DE-sub')


while True:

    # Display message received from producer
    msg = consumer.receive()
    try:
            
        # Data streamin processin
        word = msg.data()
        capWord = word.upper()
        data = data +  '' + capWord
        lastMsg_time = time.time()
        print("Received message : '%s'" % data)
        consumer.acknowledge(msg)
    except:
        consumer.negative_acknowledge(msg)
        
print("Final message : '%s'" % data)

# Destroy pulsar clien
client.close()

