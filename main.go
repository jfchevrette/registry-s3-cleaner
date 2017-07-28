package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/caarlos0/spin"
)

type s3client struct {
	sess       *session.Session
	svc        *s3.S3
	downloader *s3manager.Downloader
}

type repoData struct {
	blobs map[string]bool
}

func getObjectContent(c s3client, bucket, key string) (string, error) {
	buf := &aws.WriteAtBuffer{}
	n, err := c.downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	return string(buf.Bytes()[:n]), err
}

func sha256FromBlobKey(key string) (string, error) {
	// blobs data path looks like: docker/registry/v2/blobs/sha256/00/0002845795df2438ed1c832452431f106e633eff7fff00bddd65863c60bcba75/data

	// if doesn't end with /data, return an error
	if !strings.HasSuffix(key, "/data") {
		return "", errors.New("invalid blob key")
	}

	// Split path into parts
	keyParts := strings.Split(key, "/")

	// Last part is the blob sha256/ID
	sha256 := keyParts[len(keyParts)-2]

	return sha256, nil
}

func isBlob(key string) bool {
	return strings.Contains(key, "blobs") && strings.HasSuffix(key, "/data")
}

func isRepoLink(key string) bool {
	return strings.Contains(key, "repositories") && strings.HasSuffix(key, "/link")
}

func readRepo(c s3client, bucket string) (*repoData, error) {
	rd := &repoData{
		blobs: map[string]bool{},
	}

	// List blobs
	err := c.svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String("docker/registry/v2/blobs"),
	}, func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
		for _, obj := range p.Contents {

			key := *obj.Key
			if !isBlob(key) {
				continue
			}

			sha256, err := sha256FromBlobKey(key)
			if err != nil {
				continue
			}

			rd.blobs[sha256] = false
		}
		return true
	})

	// List repository links
	err = c.svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String("docker/registry/v2/repositories"),
	}, func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
		for _, obj := range p.Contents {

			key := *obj.Key
			if !isRepoLink(key) {
				continue
			}

			ld, err := getObjectContent(c, bucket, key)
			if err != nil {
				continue
			}
			sha256 := strings.TrimPrefix(ld, "sha256:")

			if _, ok := rd.blobs[sha256]; ok {
				rd.blobs[sha256] = true
			}
		}
		return true
	})

	return rd, err
}

func main() {
	bucket := os.Getenv("REGISTRY_BUCKET")

	// create and start a spinner
	s := spin.New("%s Reading docker repository metadata ...")
	s.Start()
	defer s.Stop()

	// create s3 client, initiate session, s3 service and download manager
	c := s3client{}
	c.sess = session.Must(session.NewSession())

	c.svc = s3.New(c.sess, &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	c.downloader = s3manager.NewDownloaderWithClient(c.svc)

	// read repository metadata
	repoData, err := readRepo(c, bucket)
	if err != nil {
		log.Fatalf("error reading repo: %v\n", err)
		os.Exit(1)
	}

	// count blobs and usedBlobs
	blobCount := 0
	usedBlobCount := 0
	for k, v := range repoData.blobs {
		fmt.Println(k, v)
		blobCount++
		if v {
			usedBlobCount++
		}
	}
	fmt.Println("Total blobs found:", blobCount)
	fmt.Println("Blobs used by manifests:", usedBlobCount)
}
