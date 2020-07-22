package store

import (
	"github.com/prometheus/common/log"

	desc "github.com/iterum-provenance/iterum-go/descriptors"

	"github.com/iterum-provenance/sidecar/env/config"
)

// toRemoteMetadata converts a local fragment description's metadata into a remote one
func toRemoteMetadata(sidecarConf *config.Config, localMeta desc.LocalMetadata) (remoteMeta desc.RemoteMetadata) {
	remoteMeta = desc.RemoteMetadata{
		FragmentID:   localMeta.FragmentID,
		Predecessors: localMeta.Predecessors,
		Custom:       localMeta.Custom,
	}
	if localMeta.OutputChannel != nil {
		if sidecarConf == nil {
			log.Fatalf("Cannot convert output channel to remote queue, because sidecar has no config")
		}
		queue, err := sidecarConf.MapQueue(*localMeta.OutputChannel)
		if err != nil {
			log.Fatal(err)
		}
		remoteMeta.TargetQueue = &queue
	}
	return
}

// toLocalMetadata converts a remote fragment description's metadata into a local one
// It drops any existing target queue info
func toLocalMetadata(remoteMeta desc.RemoteMetadata) (localMeta desc.LocalMetadata) {
	return desc.LocalMetadata{
		FragmentID:   remoteMeta.FragmentID,
		Predecessors: remoteMeta.Predecessors,
		Custom:       remoteMeta.Custom,
	}
}
