package garbage

import (
	"os"
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/prometheus/common/log"
)

// FragmentCollector keeps track of all incoming and outgoing fragments
// It is responsible for clearing the actual data associated with the fragments
// once they are no longer needed
type FragmentCollector struct {
	Track   chan *desc.LocalFragmentDesc
	Collect chan desc.IterumID
	backlog map[desc.IterumID]*desc.LocalFragmentDesc
}

// NewFragmentCollector insatiates a FragmentCollector
func NewFragmentCollector() FragmentCollector {
	return FragmentCollector{
		Track:   make(chan *desc.LocalFragmentDesc, 10),
		Collect: make(chan desc.IterumID, 10),
		backlog: map[desc.IterumID]*desc.LocalFragmentDesc{},
	}
}

func (fgb *FragmentCollector) deleteFromDisk(frag *desc.LocalFragmentDesc) {
	for _, fileDesc := range frag.Files {
		err := os.Remove(fileDesc.LocalPath)
		if err != nil {
			log.Errorf("Could not delete file from local volume due to '%v'", err)
		}
	}
}

// StartBlocking listens on the two channels for new messages:
// one channel for fragments to track,
// and one for unneeded ones that can now be removed
func (fgb *FragmentCollector) StartBlocking() {
	for fgb.Track != nil || fgb.Collect != nil {
		select {
		// If a message comes in to be tracked it is safed until later
		case fragDesc, ok := <-fgb.Track:
			if !ok {
				fgb.Track = nil
				continue
			}
			fgb.backlog[fragDesc.Metadata.FragmentID] = fragDesc
		// If a message comes in to be collected it is ready to be deleted from the disk
		case fragID, ok := <-fgb.Collect:
			if !ok {
				fgb.Collect = nil
				continue
			}
			if _, ok := fgb.backlog[fragID]; !ok {
				log.Warnf("FragmentCollector got a fragmentID that was not tracked: '%v'", fragID)
				continue
			}
			go fgb.deleteFromDisk(fgb.backlog[fragID])
			delete(fgb.backlog, fragID)
		}
	}
	if len(fgb.backlog) != 0 {
		log.Errorln("FragmenterGarbageCollector.backlog is not empty at end of lifecycle, unremoved files remain")
	}
	log.Infof("Both input channels are closed, finishing up FragmentCollector")
}

// Start asychronously calls StartBlocking via Gorouting
func (fgb *FragmentCollector) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fgb.StartBlocking()
	}()
}
