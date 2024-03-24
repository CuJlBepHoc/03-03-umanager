package htmlmeta

import (
	"context"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Meta
		wantErr  bool
	}{
		{
			name:     "test_empty_reader",
			input:    "",
			expected: Meta{},
		},
		{
			name: "test_with_all_attributes",
			input: `<html>
						<head>
							<title>Test Title</title>
							<meta name="description" content="Test Description">
							<meta name="keywords" content="keyword1, keyword2, keyword3">
						</head>
						<body>
							<h1>Hello, World!</h1>
						</body>
					</html>`,
			expected: Meta{
				Title:       "Test Title",
				Description: "Test Description",
				Tags:        []string{"keyword1", "keyword2", "keyword3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				r := strings.NewReader(tt.input)
				got, err := Parse(context.Background(), r)

				if (err != nil) != tt.wantErr {
					t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if got.Title != tt.expected.Title {
					t.Errorf("Parse() got = %v, want %v", got.Title, tt.expected.Title)
				}

				if got.Description != tt.expected.Description {
					t.Errorf("Parse() got = %v, want %v", got.Description, tt.expected.Description)
				}

				if len(got.Tags) != len(tt.expected.Tags) {
					t.Errorf(
						"Parse() got = %v keywords, want %v keywords", len(got.Tags), len(tt.expected.Tags),
					)
					return
				}

				for i := range got.Tags {
					if got.Tags[i] != tt.expected.Tags[i] {
						t.Errorf("Parse() got = %v, want %v", got.Tags[i], tt.expected.Tags[i])
					}
				}
			},
		)
	}
}
