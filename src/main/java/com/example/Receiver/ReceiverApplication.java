package com.example.Receiver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.*;
import com.azure.storage.blob.*;
import java.util.function.Consumer;

import com.azure.identity.*;

@SpringBootApplication
public class ReceiverApplication {
 private static final String namespaceName = "a216425-t3-musea2-evhns-ehnscl.servicebus.windows.net";
 private static final String eventHubName = "a216425-t3-musea2-evh-ehcl1";
 public static void main(String[] args) throws Exception{

 SpringApplication.run(ReceiverApplication.class, args);

 // create a token using the ManagedIdentityCredential lib
 ManagedIdentityCredential credential = new ManagedIdentityCredentialBuilder().build();


 EventHubConsumerAsyncClient consumer = new EventHubClientBuilder()
 .credential(credential)
 .fullyQualifiedNamespace(namespaceName)
 .eventHubName(eventHubName)
 .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
 .buildAsyncConsumerClient();
 // Receives events from all partitions from the beginning of each partition.
 consumer.receive(true).subscribe(partitionEvent -> {
 PartitionContext context = partitionEvent.getPartitionContext();
 EventData event = partitionEvent.getData();
 System.out.println(event.getBodyAsString());
 System.out.printf("Event %s is from partition %s%n.", event.getSequenceNumber(), context.getPartitionId());
 });

// Create an event processor client to receive and process events and errors.
// EventProcessorClient eventProcessorClient = new EventProcessorClientBuilder()
// .fullyQualifiedNamespace(namespaceName)
// .eventHubName(eventHubName)
// .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
// .processEvent(PARTITION_PROCESSOR)
// .processError(ERROR_HANDLER)
// .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient))
// .credential(credential)
// .buildEventProcessorClient();



 System.out.println("Exiting process");
 }

 public static final Consumer<EventContext> PARTITION_PROCESSOR = eventContext -> {
 PartitionContext partitionContext = eventContext.getPartitionContext();
 EventData eventData = eventContext.getEventData();

 System.out.printf("Processing event from partition %s with sequence number %d with body: %s%n",
 partitionContext.getPartitionId(), eventData.getSequenceNumber(), eventData.getBodyAsString());

 // Every 10 events received, it will update the checkpoint stored in Azure Blob Storage.
 if (eventData.getSequenceNumber() % 10 == 0) {
 eventContext.updateCheckpoint();
 }
 };

 public static final Consumer<ErrorContext> ERROR_HANDLER = errorContext -> {
 System.out.printf("Error occurred in partition processor for partition %s, %s.%n",
 errorContext.getPartitionContext().getPartitionId(),
 errorContext.getThrowable());
 };

}