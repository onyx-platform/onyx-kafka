#### 0.10.0.0-beta9
* Kafka input plugin now supports reading from multiple partitions with a single peer.

#### 0.10.0.0-beta2
* Greatly improve kafka output performance.

#### 0.9.15.1
* Major bug fix: peer recovery may lose messages that were read but were unacked

#### 0.9.11.1
* Major bug fix: `:kafka/offset-reset` was ignored unless `:kafka/force-reset?` was set.

#### 0.9.11.0
* **BREAKING CHANGE** `:kafka/offset-reset` `:smallest` was renamed to `:earliest`, `:largest` was renamed to `:latest`
* *WARNING*, task-maps are now schema validated for correctness at run-time.

#### 0.9.10.1
* **Major bug fix** Resumption of checkpointing was only occurring between jobs using the same `:onyx/tenancy-id`.
* Added ability to pass in arbitrary consumer and producer options via `:kafka/producer-opts` and `:kakfa/consumer-opts`
* Greatly improve performance of kafka write plugin.
* Kill Onyx jobs, rather than rebooting peers, on several unrecoverable plugin errors.

#### 0.9.10.0
* **BREAKING CHANGE** `:kafka/chan-capacity` has been removed as there is no longer a separate reader thread producing messages
* **BREAKING CHANGE** `:kafka/empty-read-back-off` has been removed as there is no longer a separate reader thread producing messages. Tweak `:onyx/batch-timeout` instead, as this will change the kafka consumer poll time.
* **BREAKING CHANGE** `:kafka/fetch-size` deprecated. Use `:kafka/receive-buffer-bytes`.
* Added `:kafka/start-offsets`, allowing a job to be started at a particular offset on each partition that is being read from.
* Fixed several issues with unclosed ZooKeeper connections
* Large performance improvements were made.
* Moved franzy-admin and franzy-embedded server dependencies to dev dependencies to reduce dependency bloat

#### 0.9.9.0
* **BREAKING CHANGE** Upgrade to use the new Kafka 0.9 client, deprecating the 0.8 client at [onyx-kafka-0.8](http://www.github.com/onyx-platform/onyx-kafka-0.8).

#### 0.9.7.0
* Defensively reduce chances of deadlock.

# 0.9.0.2
* Fixed typo in README

# 0.8.7
* Add :kafka/wrap-with-metadata? input task-map option.

# 0.8.6
* Fixed issue where exception was not passed back from the kafka reader thread to read-batch. This meant that peers could become zombies and stop reading from the topic.


# 0.7.2.3
Support auto-assigned partitions.

# 0.7.2.1
* *BREAKING CHANGE* write-messages now takes segments in the form `{:message message-body}` or `{:message message-body :key message-key}`. 
* Embedded Kafka server and take-segments test utils function are now included
