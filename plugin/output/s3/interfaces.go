package s3

import "github.com/minio/minio-go"

type ObjStoreFabricInterface interface {
	NewObjStoreClient(endpoint, accessKeyID, secretAccessKey string, secure bool) (ObjectStoreClient, error)
}

type ObjectStoreClient interface {
	BucketExists(bucketName string) (bool, error)
	FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error)
}
