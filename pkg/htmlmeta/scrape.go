package htmlmeta

import (
	"context"
	"fmt"
	"io"
	"strings"

	"golang.org/x/net/html"
)

type Meta struct {
	Title       string   `json:"title"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"keywords,omitempty"`
}

func Parse(ctx context.Context, r io.Reader) (*Meta, error) {
	doc, err := html.Parse(r)
	if err != nil {
		return nil, fmt.Errorf("html.Parse: %w", err)
	}

	var m Meta

	if err := traverse(ctx, doc, &m); err != nil {
		return nil, fmt.Errorf("traverse: %w", err)
	}

	return &m, nil
}

func traverse(ctx context.Context, n *html.Node, m *Meta) error {
	if n.Type == html.ElementNode {
		switch n.Data {
		case "meta":
			parseMeta(m, n)
		case "title":
			if n.FirstChild != nil {
				m.Title = n.FirstChild.Data
			}
		default:
		}
	}

	done := m.Title != "" && len(m.Tags) > 0 && m.Description != ""
	if done {
		return nil
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := traverse(ctx, c, m); err != nil {
			return err
		}
	}

	return nil
}

func parseMeta(m *Meta, n *html.Node) {
	if len(n.Attr) < 2 {
		return
	}

	var content string

	var (
		isKeywords    bool
		isDescription bool
	)

	for _, attr := range n.Attr {
		switch {
		case attr.Key == "name" && strings.ToLower(attr.Val) == "keywords":
			isKeywords = true
		case attr.Key == "name" && strings.ToLower(attr.Val) == "description":
			isDescription = true
		case attr.Key == "content":
			content = attr.Val
		}
	}

	switch {
	case isDescription:
		m.Description = content
	case isKeywords:
		tags := strings.Split(content, ",")
		for idx1 := range tags {
			tags[idx1] = strings.TrimSpace(tags[idx1])
		}

		m.Tags = append(m.Tags, tags...)
	default:
	}
}
