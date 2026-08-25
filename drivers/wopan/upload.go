package template

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/errgroup"
	"github.com/OpenListTeam/wopan-sdk-go"
	"github.com/avast/retry-go"
)

const maxUploadThreads = 16

func (d *Wopan) putParallel(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	file := stream.GetFile()
	if file == nil {
		var err error
		uploadProgress := up
		file, err = stream.CacheFullAndWriter(&uploadProgress, nil)
		if err != nil {
			return err
		}
		up = uploadProgress
	}

	_, err := d.upload2CParallel(ctx, stream, file, dstDir.GetID(), up)
	return err
}

func (d *Wopan) upload2CParallel(ctx context.Context, stream model.FileStreamer, file model.File, targetDirID string, up driver.UpdateProgress) (string, error) {
	size := stream.GetSize()
	if size < 0 {
		return "", fmt.Errorf("invalid upload size: %d", size)
	}

	zoneURL := d.client.ZoneURL
	if zoneURL == "" {
		if err := d.client.InitZoneURL(); err != nil {
			return "", err
		}
		zoneURL = d.client.ZoneURL
	}
	if zoneURL == "" {
		zoneURL = wopan.DefaultZoneURL
	}

	// upload2C treats totalPart as the number of full-size chunks before the
	// final chunk. The final chunk may therefore be larger than DefaultPartSize.
	totalPart := size / wopan.DefaultPartSize
	if totalPart == 0 {
		totalPart = 1
	}
	partSize := func(partIndex int64) int64 {
		if partIndex == totalPart {
			return size - (partIndex-1)*wopan.DefaultPartSize
		}
		return wopan.DefaultPartSize
	}

	accessToken, _ := d.client.GetToken()
	fileInfo := wopan.Json{
		"spaceType":   d.getSpaceType(),
		"directoryId": targetDirID,
		"batchNo":     time.Now().Format("20060102150405"),
		"fileName":    stream.GetName(),
		"fileSize":    size,
		"fileType":    d.client.GetFileType(stream.GetName()),
	}
	if d.getSpaceType() == wopan.SpaceTypeFamily {
		fileInfo["familyId"] = d.FamilyID
	}
	fileInfoStr, err := d.client.EncryptParam(wopan.ChannelWoHome, fileInfo)
	if err != nil {
		return "", err
	}
	uniqueID := strconv.FormatInt(time.Now().UnixMilli(), 10)

	var (
		fid      string
		fidMu    sync.Mutex
		uploaded atomic.Int64
	)
	group, _ := errgroup.NewGroupWithContext(ctx, d.uploadThread,
		retry.Attempts(3),
		retry.Delay(time.Second),
		retry.DelayType(retry.BackOffDelay))
	for partIndex := int64(1); partIndex <= totalPart; partIndex++ {
		partIndex := partIndex
		partSize := partSize(partIndex)
		group.Go(func(ctx context.Context) error {
			formData := map[string]string{
				"uniqueId":    uniqueID,
				"accessToken": accessToken,
				"fileName":    stream.GetName(),
				"psToken":     "undefined",
				"fileSize":    strconv.FormatInt(size, 10),
				"totalPart":   strconv.FormatInt(totalPart, 10),
				"partSize":    strconv.FormatInt(partSize, 10),
				"partIndex":   strconv.FormatInt(partIndex, 10),
				"channel":     wopan.ChannelWoCloud,
				"directoryId": targetDirID,
				"fileInfo":    fileInfoStr,
			}

			partReader := io.NewSectionReader(file, (partIndex-1)*wopan.DefaultPartSize, partSize)
			resp := wopan.Upload2CResp{}
			res, err := d.client.NewRequest().
				SetResult(&resp).
				ForceContentType("application/json;charset=UTF-8").
				SetHeaders(map[string]string{
					"Origin":     "https://pan.wo.cn",
					"Referer":    "https://pan.wo.cn/",
					"User-Agent": wopan.DefaultUA,
				}).
				SetMultipartFormData(formData).
				SetMultipartField("file", stream.GetName(), stream.GetMimetype(), driver.NewLimitedUploadStream(ctx, partReader)).
				SetContext(ctx).
				Post(zoneURL + "/openapi/client/" + wopan.KeyUpload2C)
			if err != nil {
				return err
			}
			if res.IsError() {
				return fmt.Errorf("partIndex: %d, upload failed with http status: %d, body: %s", partIndex, res.StatusCode(), res.String())
			}
			if resp.Code != "0000" {
				return fmt.Errorf("partIndex: %d, upload failed with code: %s, msg: %s", partIndex, resp.Code, resp.Msg)
			}

			if resp.Data.Fid != "" {
				fidMu.Lock()
				fid = resp.Data.Fid
				fidMu.Unlock()
			}
			current := uploaded.Add(partSize)
			if up != nil && size > 0 {
				up(100 * float64(current) / float64(size))
			} else if up != nil {
				up(100)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return "", err
	}
	return fid, nil
}
