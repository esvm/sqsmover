package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func buildSendMessageEntries(messages []*sqs.Message) []*sqs.SendMessageBatchRequestEntry {
	var entries []*sqs.SendMessageBatchRequestEntry
	for _, message := range messages {
		messageEntry := &sqs.SendMessageBatchRequestEntry{
			MessageBody:       message.Body,
			Id:                message.MessageId,
			MessageAttributes: message.MessageAttributes,
		}

		if messageGroupId, ok := message.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]; ok {
			messageEntry.MessageGroupId = messageGroupId
		}

		if messageDeduplicationId, ok := message.Attributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]; ok {
			messageEntry.MessageDeduplicationId = messageDeduplicationId
		}

		entries = append(entries, messageEntry)
	}

	return entries
}

func reprocessMessages(svc *sqs.SQS, mainQueueUrl string, messagesToReprocess []*sqs.Message) (*sqs.SendMessageBatchOutput, error) {
	reprocessMessagesOutput, err := svc.SendMessageBatch(
		&sqs.SendMessageBatchInput{
			QueueUrl: aws.String(mainQueueUrl),
			Entries:  buildSendMessageEntries(messagesToReprocess),
		},
	)
	if err != nil {
		return nil, parseAwsError("Failed to reprocess messages to the destination", err)
	}
	if len(reprocessMessagesOutput.Failed) > 0 {
		return nil, fmt.Errorf("failed to reprocess %d messages", len(reprocessMessagesOutput.Failed))
	}

	return reprocessMessagesOutput, nil
}
