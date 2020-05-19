package lineage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/iterum-go/util"
	"github.com/prometheus/common/log"
)

// Tracker is a structure responsible for tracking lineage information and submitting
// it to a central gathering point
type Tracker struct {
	TransformationName string
	ToLineate          <-chan transmit.Serializable // desc.RemoteFragmentDesc
	endpoint           string
}

// NewTracker instantiates a new Tracker
func NewTracker(processName, managerURL, pipelineHash string, toLineate chan transmit.Serializable) Tracker {
	return Tracker{
		TransformationName: processName,
		ToLineate:          toLineate,
		endpoint:           managerURL + "/pipeline/" + pipelineHash + "/lineage/",
	}
}

func (tracker Tracker) postLineage(rfd desc.RemoteFragmentDesc) (err error) {
	defer util.ReturnErrOnPanic(&err)
	lineage := struct {
		ProcessName  string          `json:"transformation_step"`
		Predecessors []desc.IterumID `json:"predecessors"`
	}{
		ProcessName:  tracker.TransformationName,
		Predecessors: rfd.Metadata.Predecessors,
	}

	enc, err := json.Marshal(lineage)
	util.PanicIfErr(err, fmt.Sprintf("Could not marshal lineage due to '%v'", err))
	url := tracker.endpoint + string(rfd.Metadata.FragmentID)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(enc))
	util.PanicIfErr(err, fmt.Sprintf("Posting lineage failed due to '%v'", err))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Post lineage request did not return 'StatusOK', instead got '%v'", resp.StatusCode)
	}
	return nil
}

// StartBlocking starts the main loop of the Tracker
func (tracker *Tracker) StartBlocking() {
	tracked := 0
	for msg := range tracker.ToLineate {
		rfd := *msg.(*desc.RemoteFragmentDesc)
		err := tracker.postLineage(rfd)
		log.Errorln(err)
		tracked++
	}
	log.Infof("Finishing up lineage tracker. Tracked %v fragments\n", tracked)
}

// Start asychronously calls StartBlocking via Goroutine
func (tracker *Tracker) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		tracker.StartBlocking()
	}()
}
