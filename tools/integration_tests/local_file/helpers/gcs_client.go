package helpers

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

// ReadObjectFromGCS downloads the object from GCS and returns the data.
func ReadObjectFromGCS(bucket, object string) (string, error) {
	ctx := context.Background()

	// Create new storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Create storage reader to read from GCS.
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return "", fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer rc.Close()

	// Variable buf will contain the output from reader.
	buf := make([]byte, 1024)
	_, err = rc.Read(buf)
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		return "", fmt.Errorf("rc.Read: %w", err)
	}

	// Remove any extra null characters from buf before returning.
	return strings.Trim(string(buf), "\x00"), nil
}

// CreateObject creates an object with given name and content on GCS.
func CreateObject(bucket, object string, content string) error {
	ctx := context.Background()

	// Create new storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	o := client.Bucket(bucket).Object(object)
	o = o.If(storage.Conditions{DoesNotExist: true})

	// Upload an object with storage.Writer.
	wc := o.NewWriter(ctx)
	if _, err = io.WriteString(wc, content); err != nil {
		return fmt.Errorf("io.WriteSTring: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %w", err)
	}

	return nil
}
