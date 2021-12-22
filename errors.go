package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

func parseAwsError(message string, err error) error {
	if awsErr, ok := err.(awserr.Error); ok {
		return fmt.Errorf("%s. Error: %s", message, awsErr.Message())
	}

	return fmt.Errorf("%s. Error: %s", message, err.Error())
}
