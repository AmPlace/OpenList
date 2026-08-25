package template

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	streamPkg "github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/wopan-sdk-go"
)

func TestUpload2CParallelUploadsPartsConcurrently(t *testing.T) {
	const threadLimit = 3
	partSize := wopan.DefaultPartSize
	const expectedParts = int64(3)
	payload := append(
		bytes.Repeat([]byte("openlist"), int(expectedParts*partSize/8)),
		[]byte("tail")...,
	)

	var (
		active       atomic.Int32
		maxActive    atomic.Int32
		reachedLimit = make(chan struct{})
		reachedOnce  sync.Once
		release      = make(chan struct{})
		releaseOnce  sync.Once
		requestMu    sync.Mutex
		uniqueIDs    = map[string]bool{}
		partIndexes  = map[int64]bool{}
		handlerErrCh = make(chan error, 1)
	)

	roundTrip := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			old := maxActive.Load()
			if current <= old || maxActive.CompareAndSwap(old, current) {
				break
			}
		}
		if current >= threadLimit {
			reachedOnce.Do(func() { close(reachedLimit) })
		}
		<-release

		_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			handlerErrCh <- err
			return nil, err
		}
		reader := multipart.NewReader(r.Body, params["boundary"])
		fields := make(map[string]string)
		var partData []byte
		for {
			part, nextErr := reader.NextPart()
			if nextErr == io.EOF {
				break
			}
			if nextErr != nil {
				handlerErrCh <- nextErr
				return nil, nextErr
			}
			data, readErr := io.ReadAll(part)
			if readErr != nil {
				handlerErrCh <- readErr
				return nil, readErr
			}
			if part.FormName() == "file" {
				partData = data
			} else {
				fields[part.FormName()] = string(data)
			}
		}

		partIndex, err := strconv.ParseInt(fields["partIndex"], 10, 64)
		if err != nil {
			handlerErrCh <- err
			return nil, err
		}
		requestPartSize, err := strconv.ParseInt(fields["partSize"], 10, 64)
		if err != nil {
			handlerErrCh <- err
			return nil, err
		}
		totalPart, err := strconv.ParseInt(fields["totalPart"], 10, 64)
		if err != nil {
			handlerErrCh <- err
			return nil, err
		}
		start := (partIndex - 1) * partSize
		if requestPartSize != int64(len(partData)) || start < 0 || start+int64(len(partData)) > int64(len(payload)) ||
			totalPart != expectedParts || !bytes.Equal(partData, payload[start:start+int64(len(partData))]) {
			err := &uploadTestError{partIndex: partIndex, message: "part payload does not match its offset"}
			handlerErrCh <- err
			return nil, err
		}

		requestMu.Lock()
		uniqueIDs[fields["uniqueId"]] = true
		partIndexes[partIndex] = true
		requestMu.Unlock()

		responseBody, err := json.Marshal(map[string]any{
			"code": "0000",
			"data": map[string]string{"fid": "test-fid"},
			"msg":  "上传成功",
		})
		if err != nil {
			handlerErrCh <- err
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(string(responseBody))),
			Request:    r,
		}, nil
	})

	client := wopan.New(
		wopan.WithAccessToken("0123456789abcdef"),
		wopan.WithUA(wopan.DefaultUA),
		wopan.WithClient(&http.Client{Transport: roundTrip}),
	)
	client.ZoneURL = "http://unit.test"
	d := &Wopan{
		Addition:     Addition{UploadThread: strconv.Itoa(threadLimit)},
		uploadThread: threadLimit,
		client:       client,
	}
	fileStream := &streamPkg.FileStream{
		Obj:    &model.Object{Name: "test", Size: int64(len(payload))},
		Reader: bytes.NewReader(payload),
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := d.upload2CParallel(t.Context(), fileStream, fileStream.GetFile(), "0", func(float64) {})
		errCh <- err
	}()

	select {
	case <-reachedLimit:
	case <-time.After(3 * time.Second):
		releaseOnce.Do(func() { close(release) })
		t.Fatalf("upload did not reach %d concurrent requests; max observed: %d", threadLimit, maxActive.Load())
	}
	releaseOnce.Do(func() { close(release) })

	if err := <-errCh; err != nil {
		t.Fatalf("parallel upload failed: %v", err)
	}
	if maxActive.Load() > threadLimit {
		t.Fatalf("upload exceeded configured concurrency: got %d, limit %d", maxActive.Load(), threadLimit)
	}
	select {
	case err := <-handlerErrCh:
		t.Fatal(err)
	default:
	}

	requestMu.Lock()
	defer requestMu.Unlock()
	if len(uniqueIDs) != 1 {
		t.Fatalf("expected one shared uniqueId, got %d", len(uniqueIDs))
	}
	if len(partIndexes) != int(expectedParts) {
		t.Fatalf("expected %d uploaded parts, got %d", expectedParts, len(partIndexes))
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type uploadTestError struct {
	partIndex int64
	message   string
}

func (e *uploadTestError) Error() string {
	return "part " + strconv.FormatInt(e.partIndex, 10) + ": " + e.message
}
