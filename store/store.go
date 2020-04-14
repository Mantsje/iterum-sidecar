package store

import (
	"github.com/prometheus/common/log"

	"github.com/minio/minio-go/v6"
)

// Main doet niks
func Main() {
	endpoint := "localhost:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false

	// Initialize minio client object.
	client, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	// Make a new bucket called mymusic.
	bucketName := "kitties"

	// Check to see if we already own this bucket (which happens if you run this twice)
	exists, errBucketExists := client.BucketExists(bucketName)
	if errBucketExists == nil && !exists {
		err = client.MakeBucket(bucketName, "")
		if err != nil {
			log.Fatalf("Failed to create bucket due to: '%v'\n", err)
		}
	} else if errBucketExists != nil {
		log.Fatalf("Failed to check for bucket existence due to: '%v'\n", errBucketExists)
	}

	// objectName := "kitty2.jpg"
	// filePath := "../clean-project/cats/cat.2.jpg"

	// // Upload the zip file with FPutObject
	// n, err := client.FPutObject(bucketName, objectName, filePath, minio.PutObjectOptions{})

	// if err != nil {
	// 	log.Fatalln(err)
	// }
	// log.Infof("Successfully uploaded %s of size %d\n", objectName, n)

	objectName := "kitty2.jpg"
	filePath := "../clean-project/cats/cat.2.jpg"
	err = client.FGetObject(bucketName, objectName, filePath, minio.GetObjectOptions{})

	if err != nil {
		log.Fatalln(err)
	}

	log.Infof("Successfully downloaded %s\n", objectName)

}
