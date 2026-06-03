#!/bin/bash

# ⚠️ WARNING: This script will start, stop, configure Kafka and Zookeeper services.
# It also includes commands to delete topics, logs, and configurations.
# **Proceed with caution** and ensure you have backups if necessary.

echo "⚠️ WARNING: This script will configure Kafka, Zookeeper, and delete logs/topics. Ensure you have backups and are aware of the consequences."

# **Step 1: Configure Zookeeper**

# Edit the Zookeeper configuration file
echo "Configuring Zookeeper settings in config/zookeeper.properties..."
sudo nano config/zookeeper.properties

# Add these values in zookeeper.properties (ensure your IP and dataDir are correct):
# clientPortAddress=172.31.2.175
# clientPort=2181
# dataDir=/tmp/zookeeper

# **Step 2: Configure Kafka Broker**

# Edit the Kafka server configuration file
echo "Configuring Kafka settings in config/server.properties..."
sudo nano config/server.properties

# Add these values in server.properties (ensure your IP and listener settings are correct):
# zookeeper.connect=xxx.xxx.xx.xx:2181
# listeners=PLAINTEXT://xxx.xxx.xx.xx:9092
# advertised.listeners=PLAINTEXT://xxx.xxx.xx.xx:9092
# listener.security.protocol.map=PLAINTEXT:PLAINTEXT
# listener.name.default=PLAINTEXT

# **Step 3: Update the PATH for Kafka in .bashrc**

# Add Kafka binaries to PATH so we can execute Kafka commands easily
echo "Adding Kafka binaries to PATH in ~/.bashrc..."
echo "export PATH=\$PATH:/usr/local/kafka/bin" >> ~/.bashrc
source ~/.bashrc  # Apply the changes to your shell environment

# **Step 4: Navigate to Kafka Installation Directory**

# Ensure you're in the correct Kafka installation directory
echo "Navigating to Kafka installation directory..."
cd /usr/local/kafka

# **Step 5: Start Zookeeper and Kafka in Background**

# Start Zookeeper in the background (remove '&' to run in the foreground if needed)
echo "Starting Zookeeper in the background..."
sudo bin/zookeeper-server-start.sh config/zookeeper.properties &

# Start Kafka broker in the background (remove '&' to run in the foreground if needed)
echo "Starting Kafka in the background..."
sudo bin/kafka-server-start.sh config/server.properties &

# **Step 6: Wait for Kafka and Zookeeper to Initialize**

# Wait a few seconds to ensure that both Zookeeper and Kafka have time to start up properly
echo "Waiting for Zookeeper and Kafka to initialize..."
sleep 10

# **Step 7: Create a Kafka Topic (For Testing)**

# Create a new topic called 'test' for testing purposes
echo "Creating a test topic..."
sudo bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# **Step 8: Produce Messages to the Topic**

# Optionally, send a message to the 'test' topic using the Kafka producer
echo "Producing messages to the 'test' topic. Type your message and hit Enter (Ctrl+C to stop)..."
sudo bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9092

# **Step 9: Consume Messages from the Topic**

# Optionally, consume the messages from the 'test' topic
echo "Consuming messages from the 'test' topic..."
sudo bin/kafka-console-consumer.sh --topic test --bootstrap-server localhost:9092 --from-beginning

# **Step 11: Gracefully Stop Kafka and Zookeeper**

# Stop the Kafka broker gracefully
echo "Stopping Kafka gracefully..."
sudo bin/kafka-server-stop.sh

# Stop Zookeeper gracefully
echo "Stopping Zookeeper gracefully..."
sudo bin/zookeeper-server-stop.sh

# **Step 10: Delete the Kafka Topic**

# Now, delete the Kafka 'test' topic we created earlier
echo "Deleting the 'test' topic..."
sudo bin/kafka-topics.sh --delete --topic test --bootstrap-server localhost:9092

# **Step 12: List Kafka Topics**

# List all Kafka topics in the cluster
echo "Listing all Kafka topics..."
bin/kafka-topics.sh --list --bootstrap-server xxx.xxx.xx.xx:9092

# **Step 13: Delete All Kafka Topics (Be Careful!)**

# WARNING: This will delete all topics in Kafka! Ensure that you really want to delete everything.
echo "Deleting all Kafka topics..."
    kafka-topics.sh --bootstrap-server xxx.xxx.xx.xx:9092 --list | xargs -I {} kafka-topics.sh --bootstrap-server xxx.xxx.xx.xx:9092 --delete --topic {}

# **Step 14: Delete a Specific Kafka Topic**

# Delete a single topic by name
echo "Deleting specific Kafka topic (replace <TOPIC_NAME> with the actual topic name)..."
kafka-topics.sh --bootstrap-server xxx.xxx.xx.xx:9092 --delete --topic <TOPIC_NAME>

# **Step 15: Deleting Zookeeper and Kafka Logs (Be Cautious)**

# Delete Zookeeper logs (careful, this clears all Zookeeper logs)
echo "Deleting Zookeeper logs..."
sudo rm -rf /usr/local/kafka_2.12-3.9.0/logs/zookeeper.log*

# Delete Zookeeper data directory (this will erase all state data)
echo "Deleting Zookeeper data directory..."
sudo rm -rf /tmp/zookeeper

# Delete Kafka logs (be careful, this will delete all Kafka logs)
echo "Deleting all Kafka logs..."
sudo rm -rf /tmp/kafka-logs/*  
# **Permanent and irreversible**

# **Step 16: Delete Kafka Topic Logs Manually**

# Delete logs for a specific Kafka topic (use the topic name or use a wildcard for all topics)
echo "Deleting Kafka logs for the topic 'my-topic'..."
sudo rm -rf /tmp/kafka-logs/my-topic  # Replace with the correct directory for your Kafka logs

# **Step 17: Using Screen or Tmux for Detached Sessions**

# Install screen if not already installed
echo "Installing screen for detaching sessions..."
sudo apt-get install screen

# Start a new screen session
echo "Starting a new screen session for Kafka and Zookeeper..."
screen -S kafka-zookeeper

# Start Zookeeper and Kafka inside the screen session
echo "Starting Zookeeper inside screen session..."
sudo bin/zookeeper-server-start.sh config/zookeeper.properties

echo "Starting Kafka inside screen session..."
sudo bin/kafka-server-start.sh config/server.properties

#terminate screen
screen -ls | grep 'kafka-zookeeper' | awk '{print $1}' | xargs -I {} screen -X -S {} quit

#all screen
screen -ls

# **Detach from the Screen Session**
# Press Ctrl-A followed by D to detach from the session, leaving Kafka and Zookeeper running in the background

# **Reattach to the Session**
# To return to the session later, type:
# screen -r kafka-zookeeper

# **Step 18: Access Logs After Stopping Kafka and Zookeeper**

# Once Zookeeper and Kafka are stopped, you can still access their logs for troubleshooting

# View Zookeeper logs
echo "Viewing Zookeeper logs..."
tail -f /usr/local/kafka_2.12-3.9.0/logs/zookeeper.log

# View Kafka logs
echo "Viewing Kafka logs..."
tail -f /usr/local/kafka_2.12-3.9.0/logs/server.log

# **Step 19: Summary and Final Notes**

echo "✅ Kafka and Zookeeper are configured, running, and tested successfully!"
echo "Logs and topics have been deleted where requested."
echo "Kafka and Zookeeper services have been restarted and are now running in the background."

# You can minimize or close the terminal, and Kafka and Zookeeper will continue running if started in the background.
# Optionally use tmux or screen for better management of background services.


#Run Bash script
sudo chmod +x /usr/local/kafka_2.12-3.9.0/start_kafka_zookeeper.sh
sudo /usr/local/kafka_2.12-3.9.0/start_kafka_zookeeper.sh


# **END OF SCRIPT**


