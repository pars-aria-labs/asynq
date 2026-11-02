package base

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// LabelHeaderPrefix reserves a separate namespace from application headers.
const LabelHeaderPrefix = "asynq.label."

// TaskLabels returns an independent copy of the task's searchable labels.
func TaskLabels(headers map[string]string) map[string]string {
	var labels map[string]string
	for key, value := range headers {
		if strings.HasPrefix(key, LabelHeaderPrefix) {
			if labels == nil {
				labels = make(map[string]string)
			}
			labels[strings.TrimPrefix(key, LabelHeaderPrefix)] = value
		}
	}
	return labels
}

// ValidateLabels bounds the per-task indexing work and stored metadata.
func ValidateLabels(labels map[string]string) error {
	if len(labels) > 16 {
		return fmt.Errorf("a task cannot have more than 16 labels")
	}
	for key, value := range labels {
		if len(key) == 0 || len(key) > 64 || len(value) > 256 || !utf8.ValidString(key) || !utf8.ValidString(value) {
			return fmt.Errorf("label keys must be 1..64 UTF-8 bytes and values at most 256 UTF-8 bytes")
		}
		for _, c := range key {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
				return fmt.Errorf("label keys may only contain letters, digits, underscores, dots and hyphens")
			}
		}
	}
	return nil
}
