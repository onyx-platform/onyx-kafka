# 0.8.7
* Add :kafka/wrap-with-metadata? input task-map option.

# 0.8.6
* Fixed issue where exception was not passed back from the kafka reader thread to read-batch. This meant that peers could become zombies and stop reading from the topic.


# 0.7.2.3
Support auto-assigned partitions.

# 0.7.2.1
* *BREAKING CHANGE* write-messages now takes segments in the form `{:message message-body}` or `{:message message-body :key message-key}`. 
* Embedded Kafka server and take-segments test utils function are now included
