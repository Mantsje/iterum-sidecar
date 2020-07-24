// Package garbage contains the Fragment Garbage Collection functionality of the Iterum Sidecar
// it prevents the pods from bloating with all data over time by removing all files relating to fragments
// that are no longer needed. This can only be performed upon the user-defined transformations informing
// the sidecar that this action is allowed for a certain fragment.
package garbage

import (
	"os"
	"sync"
	"time"

	"github.com/prometheus/common/log"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
)

// FragmentCollector keeps track of all incoming and outgoing fragments
// It is responsible for clearing the actual data associated with the fragments
// once they are no longer needed
type FragmentCollector struct {
	Track     chan *desc.LocalFragmentDesc
	Collect   chan desc.IterumID
	backlog   map[desc.IterumID]*desc.LocalFragmentDesc
	untracked []desc.IterumID
}

// NewFragmentCollector insatiates a FragmentCollector
func NewFragmentCollector() FragmentCollector {
	return FragmentCollector{
		Track:     make(chan *desc.LocalFragmentDesc, 10),
		Collect:   make(chan desc.IterumID, 10),
		backlog:   make(map[desc.IterumID]*desc.LocalFragmentDesc),
		untracked: []desc.IterumID{},
	}
}

// deleteFromDisk removes all files assocaited with a LocalFragmentDesc from the disk
func (fgarbage *FragmentCollector) deleteFromDisk(frag *desc.LocalFragmentDesc) {
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
func (fgarbage *FragmentCollector) StartBlocking() {
	for fgarbage.Track != nil || fgarbage.Collect != nil {
		select {
		// If a message comes in to be tracked it is safed until later
		case fragDesc, ok := <-fgarbage.Track:
			if !ok {
				log.Infof("ToTrack channel of FragmentCollector was closed")
				fgarbage.Track = nil
				continue
			}
			fgarbage.backlog[fragDesc.Metadata.FragmentID] = fragDesc
		// If a message comes in to be collected it is ready to be deleted from the disk
		case fragID, ok := <-fgarbage.Collect:
			if !ok {
				log.Infof("CollectGarbage channel of FragmentCollector was closed")
				fgarbage.Collect = nil
				continue
			}
			if _, ok := fgarbage.backlog[fragID]; !ok {
				log.Warnf("FragmentCollector got a fragmentID that was not tracked: '%v', if problematic, will result in an error log later", fragID)
				fgarbage.untracked = append(fgarbage.untracked, fragID)
				continue
			}
			go fgarbage.deleteFromDisk(fgarbage.backlog[fragID])
			delete(fgarbage.backlog, fragID)
		}
	}
	// Some files and data may be removed before it is tracked due to random choice of input channel `select` consumes from
	// This eliminates all untracked files that were actually properly deleted, preventing wrong errors
	if len(fgarbage.backlog) != 0 {
		// Remove all untracked ids
		for _, untrackedID := range fgarbage.untracked {
			if _, ok := fgarbage.backlog[untrackedID]; !ok {
				log.Errorf("Untracked ID never showed up in FragmentCollector backlog: '%v'", untrackedID)
			} else {
				delete(fgarbage.backlog, untrackedID)
			}
		}
		// If the backlog is still empty, there was an actual problem
		if len(fgarbage.backlog) != 0 {
			log.Errorln("FragmenterGarbageCollector.backlog is not empty at end of lifecycle, unremoved files remain")
		}
	}
	log.Infof("Both input channels are closed, finishing up FragmentCollector")
}

// Start asychronously calls StartBlocking via Gorouting
func (fgarbage *FragmentCollector) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		fgarbage.StartBlocking()
		log.Infof("fgarbage ran for %v", time.Now().Sub(startTime))
	}()
}
