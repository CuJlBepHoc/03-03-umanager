package v1

import (
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/api/apiv1"
)

type serverInterface interface {
	apiv1.ServerInterface
}

var _ serverInterface = (*Handler)(nil)

func New(usersRepository usersClient, linksRepository linksClient) *Handler {
	return &Handler{usersHandler: newUsersHandler(usersRepository), linksHandler: newLinksHandler(linksRepository)}
}

type Handler struct {
	*usersHandler
	*linksHandler
}
