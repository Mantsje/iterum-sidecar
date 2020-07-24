# Iterum Sidecar

---

If you haven't done so already, check out the [main repository](https://github.com/iterum-provenance/iterum) with introductory material.
Additionally, if you have not done so, this package has many dependencies on the shared library called [iterum-go](https://github.com/iterum-provenance/iterum-go). It contains shared/generalized dependencies of this sidecar, the [fragmenter-sidecar](https://github.com/iterum-provenance/fragmenter-sidecar) and the [combiner](https://github.com/iterum-provenance/combiner)

---
## General overview
The sidecar is an application that is attached to every transformation step in a pipeline. It acts as a decoupled interface between the transformation step and the rest of the Iterum stack. It abstracts away from more complex topics such as message queue interaction and dealing with distributed storage in order to keep individual language libraries (such as [Pyterum](https://github.com/iterum-provenance/pyterum) as small as possible. The general flow of a transformation step is shown in the image below, along with a more detailed explanation.

![Alt text](./resources/sidecar-arch.svg)

1. The sidecar pulls messages from the message queue containing remote fragment descriptions
2. Then the related data is downloaded from the distributed storage
3. This data is stored on a data volume shared by the two containers in this pod
4. Once downloaded the sidecar lets the transformation know a new message with data is ready to be processed using UNIX sockets
5. The transformation reads the now available data from the data volume
6. Processes its contents and writes the results back to the volume
7. It then informs the sidecar of the completion of processing this fragment
8. The sidecar then gathers the data from the volume again
9. It sends provenance information to the provenance tracker
10. It uploads the processed data to the distributed storage
11. Finally it posts a new remote fragment description on the message queue for the next step in the pipeline

---
## Sidecar structure
The sidecar itself consists of multiple smaller units wrapped up in goroutines. Each of them communicate with their dependents using channels. Most information that is communicated are the various types of `Desc` types, such as `LocalFragmentDesc`, `LocalFileDesc`, `RemoteFragmentDesc`, `RemoteFileDesc`. These are often wrapped up in custom types within their relevant packages such that information specific to that goroutine can be added without it leaving its context (see `messageq/mq_fragment_desc.go` and `socket/fragment_desc` as examples of this).

The general application flow can be observed in `main.go`, though the implementation of each goroutine is somewhat more intricate:
The implementation of many elements are shared and so stored in the shared `iterum-go` repo. For documentation on these see that repository.
This is how this sidecar links together these elements

1. `messageq.Listener` retrieves messages from the MQ, but awaits acknowledgements for a later time
2. Parsed messages are converted into RemoteFragmentDesc that are passed on to the `DownloadManager`
3. This manager distributes the work to download instances that download all files of a fragment
4. Upon completion the now LocalFragmentDesc (since its downloaded to the disk) is passed on to the transformation step input `Socket`
5. This input channel posts messages on a UNIX socket file processed by the transformation
6. At some point the transformation sends responses back. These messages can have 2 types: done_with or new LocalFragmentDesc. 
    * done_with indicates that a LocalFragment is no longer used and so the fragmentGarbage collecter can remove it from disk
    This ensures that the volume does not bloat with the entire data set over time. 
    Also the messages of the MQListener are acknowledged now, since they are processed sucessfully
    * a new LocalFragmentDesc implies the result of some processed fragment(s).
7. Inbound fragments are passed to the `UploadManager` which uploads all the data to the distributed MinIO storage
8. The now new RemoteFragmentDesc is send on to the `messageq.Sender` which posts it as a message for the next transformations

* The `UpstreamChecker` is the routine reponsible for checking when a transformation can go down.
    It prompts the manager for whether all previous steps have completed.
    Once this returns yes, the MQListener is informed that no new messages will be posted on the queue
    This makes the Listener await the current set of messages left in the queue.
    Once all have been consumed it closes some outgoing channels except for the acknowledger
    This chains throughout the other go routines, once a routine's input channel closes it finishes up and goes down.
* The `ConfigDownloader` is responsible for downloading config file from the Daemon that the transformation step may need
* The `LineageTracker` receives messages posted on the MQ from the MQSender and generates according lineage information and posts it on separate channel 

---

## Code documentation

The documentation of code is left to code files themselves. They have been set up to work with Godoc, which can be achieved by running `godoc` and navigating to `http://localhost:6060/pkg/github.com/iterum-provenance/cli/`.