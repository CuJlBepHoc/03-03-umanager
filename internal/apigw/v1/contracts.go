package v1

import (
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/pb"
)

type usersClient interface {
	pb.UserServiceClient
}

type linksClient interface {
	pb.LinkServiceClient
}
