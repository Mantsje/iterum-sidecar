package lineage

import (
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/sidecar/messageq"
	"github.com/prometheus/common/log"
)

// MqTracker posts lineage information to a designated message queue queue
type MqTracker struct {
	TransformationName string
	PipelineHash       string
	ToLineate          <-chan transmit.Serializable // desc.RemoteFragmentDesc
	Sender             messageq.SimpleSender
}

// NewMqTracker instantiates a new MqTracker
func NewMqTracker(processName, pipelineHash, brokerURL string, toLineate chan transmit.Serializable) MqTracker {
	return MqTracker{
		processName,
		pipelineHash,
		toLineate,
		messageq.NewSimpleSender(make(chan transmit.Serializable, 10), brokerURL, pipelineHash+"-lineage"),
	}
}

// StartBlocking starts the main loop of the Tracker
func (tracker MqTracker) StartBlocking() {
	tracked := 0
	for msg := range tracker.ToLineate {
		rfd := *msg.(*desc.RemoteFragmentDesc)
		// Send the lineage message to be published
		tracker.Sender.ToSend <- &Message{tracker.TransformationName, rfd.Metadata.Predecessors}
		tracked++
	}
	log.Infof("Finishing up mq lineage tracker. Tracked %v fragments\n", tracked)
	close(tracker.Sender.ToSend)
}

// Start asychronously calls StartBlocking via Goroutine
func (tracker MqTracker) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		tracker.StartBlocking()
	}()
}
