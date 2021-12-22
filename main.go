package main

import (
	"fmt"
	"strconv"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fatih/color"
	"github.com/tj/go-progress"
	"github.com/tj/go/term"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	dlqName  = kingpin.Flag("source", "The dead letter queue to move messages from.").Short('s').Required().String()
	mainQueue = kingpin.Flag("destination", "The main queue to move messages to.").Short('d').Required().String()
	region    = kingpin.Flag("region", "The AWS region the queues are created. Default is us-east-1.").Short('r').Default("us-east-1").String()
)

const (
	visibilityTimeout = 2
	waitTimeSeconds = 0
	maxNumberOfMessages = 10
	progressBarSize = 40
)

func main() {
	fmt.Println()
	defer fmt.Println()

	kingpin.UsageTemplate(kingpin.CompactUsageTemplate)
	kingpin.Parse()

	options := session.Options{}
	if region != nil {
		options.Config = aws.Config{Region: aws.String(*region)}
	}

	sess, err := session.NewSessionWithOptions(options)
	if err != nil {
		panic(fmt.Errorf("unable to create AWS session for region %s\n", *region))
		return
	}

	svc := sqs.New(sess)

	dlqUrl, err := getQueueUrl(svc, *dlqName)
	if err != nil {
		panic(parseAwsError("Failed to resolve DLQ queue " + dlqUrl, err))
		return
	}

	mainQueueUrl, err := getQueueUrl(svc, *mainQueue)
	if err != nil {
		panic(parseAwsError("Failed to resolve main queue " + mainQueueUrl, err))
		return
	}

	queueAttributes, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqUrl),
		AttributeNames: []*string{aws.String("All")},
	})
	if err != nil {
		panic(parseAwsError("Failed to resolve DLQ attributes", err))
		return
	}

	totalMessages, _ := strconv.Atoi(*queueAttributes.Attributes["ApproximateNumberOfMessages"])
	if totalMessages == 0 {
		log.Info("DLQ is empty.")
		return
	}

	moveMessages(dlqUrl, mainQueueUrl, svc, totalMessages)
}

func moveMessages(dlqUrl string, mainQueueUrl string, svc *sqs.SQS, totalMessages int) {
	fmt.Println(color.New(color.FgCyan).Sprintf("Starting to move messages..."))
	term.HideCursor()
	render := term.Renderer()
	defer term.ShowCursor()

	progressBar := buildProgressBar(totalMessages)
	reprocessedMessages := 0

	for {
		if reprocessedMessages == totalMessages {
			fmt.Printf("Moved %s messages. Done.\n", strconv.Itoa(totalMessages))
			return
		}

		output, err := svc.ReceiveMessage(buildReceiveMessagesInput(dlqUrl))
		if len(output.Messages) == 0 {
			fmt.Println("There aren't remaining messages to move.")
			return
		}

		if err != nil {
			panic(parseAwsError("Failed to receive messages", err))
			return
		}

		messagesToReprocess := output.Messages
		if len(output.Messages)+reprocessedMessages > totalMessages {
			messagesToReprocess = output.Messages[0 : totalMessages-reprocessedMessages]
		}

		reprocessMessagesOutput, err := reprocessMessages(svc, mainQueueUrl, messagesToReprocess)
		if err != nil {
			panic(err)
			return
		}

		if len(reprocessMessagesOutput.Successful) == len(messagesToReprocess) {
			err = deleteReprocessedMessages(svc, dlqUrl, messagesToReprocess)
			if err != nil {
				panic(err)
				return
			}

			reprocessedMessages += len(messagesToReprocess)
		}

		if reprocessedMessages > totalMessages {
			progressBar.Total = float64(reprocessedMessages)
		}

		progressBar.ValueInt(reprocessedMessages)
		render(progressBar.String())
	}
}

func buildProgressBar(totalMessages int) *progress.Bar {
	b := progress.NewInt(totalMessages)
	b.Width = progressBarSize
	b.StartDelimiter = color.New(color.FgCyan).Sprint("|")
	b.EndDelimiter = color.New(color.FgCyan).Sprint("|")
	b.Filled = color.New(color.FgCyan).Sprint("█")
	b.Empty = color.New(color.FgCyan).Sprint("░")
	b.Template(`		{{.Bar}} {{.Text}}{{.Percent | printf "%3.0f"}}%`)

	return b
}

func buildReceiveMessagesInput(dlqUrl string) *sqs.ReceiveMessageInput {
	return &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqUrl),
		VisibilityTimeout:     aws.Int64(visibilityTimeout),
		WaitTimeSeconds:       aws.Int64(waitTimeSeconds),
		MaxNumberOfMessages:   aws.Int64(maxNumberOfMessages),
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameMessageGroupId),
			aws.String(sqs.MessageSystemAttributeNameMessageDeduplicationId)},
	}
}
