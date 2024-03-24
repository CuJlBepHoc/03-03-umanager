package httputil

import (
	"net/http"

	"google.golang.org/grpc/codes"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/api/apiv1"
)

func ConvertHTTPToErrorCode(code int) apiv1.ErrorCode {
	switch code {
	case http.StatusBadRequest:
		return apiv1.BadRequest
	case http.StatusInternalServerError:
		return apiv1.InternalServerError
	case http.StatusRequestEntityTooLarge:
		return apiv1.BadRequest
	case http.StatusUnsupportedMediaType:
		return apiv1.BadRequest
	case http.StatusConflict:
		return apiv1.Conflict
	}
	return apiv1.InternalServerError
}

func ConvertGRPCToErrorCode(grpcCode codes.Code) apiv1.ErrorCode {
	switch grpcCode {
	case codes.Internal, codes.Unknown, codes.DataLoss:
		return apiv1.InternalServerError
	case codes.NotFound:
		return apiv1.NotFound
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return apiv1.BadRequest
	case codes.Aborted, codes.AlreadyExists:
		return apiv1.Conflict
	}

	return apiv1.InternalServerError
}

func ConvertGRPCCodeToHTTP(grpcCode codes.Code) int {
	switch grpcCode {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return http.StatusRequestTimeout
	case codes.Unknown:
		return http.StatusInternalServerError
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.FailedPrecondition:
		return http.StatusBadRequest
	case codes.Aborted:
		return http.StatusConflict
	case codes.OutOfRange:
		return http.StatusBadRequest
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Internal:
		return http.StatusInternalServerError
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DataLoss:
		return http.StatusInternalServerError
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}
