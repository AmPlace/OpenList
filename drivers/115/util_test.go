package _115

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/go-resty/resty/v2"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestLinkUsesVideoResponseCookie(t *testing.T) {
	restyClient := resty.NewWithClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host != "webapi.115.com" || req.URL.Path != "/files/video" {
				t.Fatalf("unexpected request URL: %s", req.URL.String())
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header: http.Header{
					"Content-Type": []string{"application/json"},
					"Set-Cookie":   []string{"video_token=test-value; Domain=115.com; Path=/video"},
				},
				Body:    io.NopCloser(strings.NewReader(`{"state":true,"origin_file_url":"https://cdn.example/video.mp4"}`)),
				Request: req,
			}, nil
		}),
	})
	d := &Pan115{
		Addition: Addition{UseVideoLink: true},
		client:   driver115.New(driver115.WithRestyClient(restyClient)),
	}

	link, err := d.Link(context.Background(), &FileObj{
		File: driver115.File{PickCode: "pick-code"},
	}, model.LinkArgs{})
	if err != nil {
		t.Fatalf("Link returned error: %v", err)
	}
	if link.URL != "https://cdn.example/video.mp4" {
		t.Fatalf("unexpected URL: %s", link.URL)
	}
	if got := link.Header.Get("Cookie"); got != "video_token=test-value" {
		t.Fatalf("unexpected Cookie header: %s", got)
	}
}
