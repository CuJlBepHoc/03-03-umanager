package v1

import (
	"net/http"
)

func newUsersHandler(usersClient usersClient) *usersHandler {
	return &usersHandler{client: usersClient}
}

type usersHandler struct {
	client usersClient
}

func (h *usersHandler) GetUsers(w http.ResponseWriter, r *http.Request) {
	// TODO implement me
	w.WriteHeader(http.StatusNotImplemented)
}

func (h *usersHandler) PostUsers(w http.ResponseWriter, r *http.Request) {
	// TODO implement me
	w.WriteHeader(http.StatusNotImplemented)
}

func (h *usersHandler) DeleteUsersId(w http.ResponseWriter, r *http.Request, id string) {
	// TODO implement me
	w.WriteHeader(http.StatusNotImplemented)
}

func (h *usersHandler) GetUsersId(w http.ResponseWriter, r *http.Request, id string) {
	// TODO implement me
	w.WriteHeader(http.StatusNotImplemented)
}

func (h *usersHandler) PutUsersId(w http.ResponseWriter, r *http.Request, id string) {
	// TODO implement me
	w.WriteHeader(http.StatusNotImplemented)
}
