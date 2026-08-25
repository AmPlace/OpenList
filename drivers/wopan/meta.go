package template

import (
	"encoding/json"
	"strconv"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

// UploadThreadValue accepts both the legacy string form and the numeric form
// written by the settings UI.
type UploadThreadValue int

func (v *UploadThreadValue) UnmarshalJSON(data []byte) error {
	var number int
	if err := json.Unmarshal(data, &number); err == nil {
		*v = UploadThreadValue(number)
		return nil
	}

	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	*v = UploadThreadValue(number)
	return nil
}

type Addition struct {
	// Usually one of two
	driver.RootID
	// define other
	RefreshToken string            `json:"refresh_token" required:"true"`
	FamilyID     string            `json:"family_id" help:"Keep it empty if you want to use your personal drive"`
	SortRule     string            `json:"sort_rule" type:"select" options:"name_asc,name_desc,time_asc,time_desc,size_asc,size_desc" default:"name_asc"`
	UploadThread UploadThreadValue `json:"upload_thread" type:"number" default:"1" help:"Number of concurrent part uploads (1-16)"`

	AccessToken string `json:"access_token"`
}

var config = driver.Config{
	Name:              "WoPan",
	DefaultRoot:       "0",
	NoOverwriteUpload: true,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &Wopan{}
	})
}
