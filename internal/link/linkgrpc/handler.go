package linkgrpc

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/pb"
)

var _ pb.LinkServiceServer = (*Handler)(nil)

func New(linksRepository linksRepository, timeout time.Duration, publisher amqpPublisher) *Handler {
	return &Handler{linksRepository: linksRepository, timeout: timeout, pub: publisher}
}

type Handler struct {
	pb.UnimplementedLinkServiceServer
	linksRepository linksRepository
	pub             amqpPublisher
	timeout         time.Duration
}

func (h Handler) GetLinkByUserID(ctx context.Context, id *pb.GetLinksByUserId) (*pb.ListLinkResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	list, err := h.linksRepository.FindByUserID(ctx, id.UserId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	respList := make([]*pb.Link, 0, len(list))

	for _, l := range list {
		respList = append(
			respList, &pb.Link{
				Id:        l.ID.Hex(),
				Title:     l.Title,
				Url:       l.URL,
				Images:    l.Images,
				Tags:      l.Tags,
				UserId:    l.UserID,
				CreatedAt: l.CreatedAt.Format(time.RFC3339),
				UpdatedAt: l.UpdatedAt.Format(time.RFC3339),
			},
		)
	}

	return &pb.ListLinkResponse{Links: respList}, nil
}

func (h Handler) CreateLink(ctx context.Context, request *pb.CreateLinkRequest) (*pb.Empty, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	objectID, err := primitive.ObjectIDFromHex(request.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if _, err := h.linksRepository.Create(
		ctx, database.CreateLinkReq{
			ID:     objectID,
			URL:    request.Url,
			Title:  request.Title,
			Tags:   request.Tags,
			Images: request.Images,
			UserID: request.UserId,
		},
	); err != nil {
		if errors.Is(err, database.ErrConflict) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Сообщение которое отправляем в очередь
	type message struct {
		ID string `json:"id"`
	}

	// implement me
	// h.pub.Publish()

	return &pb.Empty{}, nil
}

func (h Handler) GetLink(ctx context.Context, request *pb.GetLinkRequest) (*pb.Link, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	objectID, err := primitive.ObjectIDFromHex(request.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	l, err := h.linksRepository.FindByID(ctx, objectID)
	if err != nil {
		if errors.Is(err, database.ErrNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.Link{
		Id:        l.ID.Hex(),
		Title:     l.Title,
		Url:       l.URL,
		Images:    l.Images,
		Tags:      l.Tags,
		UserId:    l.UserID,
		CreatedAt: l.CreatedAt.Format(time.RFC3339),
		UpdatedAt: l.UpdatedAt.Format(time.RFC3339),
	}, nil
}

func (h Handler) UpdateLink(ctx context.Context, request *pb.UpdateLinkRequest) (*pb.Empty, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	objectID, err := primitive.ObjectIDFromHex(request.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if _, err := h.linksRepository.Update(
		ctx, database.UpdateLinkReq{
			ID:     objectID,
			URL:    request.Url,
			Title:  request.Title,
			Tags:   request.Tags,
			Images: request.Images,
			UserID: request.UserId,
		},
	); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.Empty{}, nil
}

func (h Handler) DeleteLink(ctx context.Context, request *pb.DeleteLinkRequest) (*pb.Empty, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	objectID, err := primitive.ObjectIDFromHex(request.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := h.linksRepository.Delete(ctx, objectID); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.Empty{}, nil
}

func (h Handler) ListLinks(ctx context.Context, request *pb.Empty) (*pb.ListLinkResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	list, err := h.linksRepository.FindAll(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	respList := make([]*pb.Link, 0, len(list))

	for _, l := range list {
		respList = append(
			respList, &pb.Link{
				Id:        l.ID.Hex(),
				Title:     l.Title,
				Url:       l.URL,
				Images:    l.Images,
				Tags:      l.Tags,
				UserId:    l.UserID,
				CreatedAt: l.CreatedAt.Format(time.RFC3339),
				UpdatedAt: l.UpdatedAt.Format(time.RFC3339),
			},
		)
	}

	return &pb.ListLinkResponse{Links: respList}, nil
}
