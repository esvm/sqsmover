package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func buildDeleteMessageEntries(messages []*sqs.Message) []*sqs.DeleteMessageBatchRequestEntry {
	var entries []*sqs.DeleteMessageBatchRequestEntry
	for _, message := range messages {
		entries = append(entries, &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		})
	}

	return entries
}

func deleteReprocessedMessages(svc *sqs.SQS, dlqUrl string, reprocessedMessages []*sqs.Message) error {
	deleteReprocessedMessagesResult, err := svc.DeleteMessageBatch(
		&sqs.DeleteMessageBatchInput{
			Entries:  buildDeleteMessageEntries(reprocessedMessages),
			QueueUrl: aws.String(dlqUrl),
		},
	)
	if err != nil {
		return parseAwsError("Failed to delete messages from DLQ", err)
	}

	if len(deleteReprocessedMessagesResult.Failed) > 0 {
		return fmt.Errorf("fail to delete %d messages from DLQ", deleteReprocessedMessagesResult.Failed)
	}

	return nil
}
