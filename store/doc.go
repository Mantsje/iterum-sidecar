// Package store contains the go routines for downloading data from and to Minio.
// a Downloader is responsible for downloading a fragment.
// The DownloadManager spawns a Downloader subroutine for each fragment that should be downloaded.
// This Downloader submits requests for each file to be downloaded and tracks its progress.
// Upon completion it is send on to the next part of the sidecar-pipeline.
// A similar argument can be made for Uploader and UploaderManager.
//
// Important parts of this package are the conversion of LocalFragmentDesc to RemoteFragmentDesc and vice versa.
// The files are up/downloaded to/from MinIO instance and downloads are stored on the local shared volume of the pod.
//
// On branch `worker-pool` this has been replaced with downloading and uploading only
// a certain amount of files at a time in order to limit the amount of concurrent requests send out.
// This feature has not been investigated enough to be the master version yet, but is targeted to be so.
package store
