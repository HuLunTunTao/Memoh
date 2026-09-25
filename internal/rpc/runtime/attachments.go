package runtime

import (
	"context"
	"crypto/md5" //nolint:gosec // compatibility digest required by the Weixin upload protocol
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/felinics/memoh/internal/attachment"
	"github.com/felinics/memoh/internal/media"
	"github.com/felinics/memoh/internal/rpc/runtimepb"
)

const attachmentChunkSize = 256 * 1024

func (s *Server) ResolveAttachment(ctx context.Context, req *runtimepb.ResolveAttachmentRequest) (*runtimepb.ResolveAttachmentResponse, error) {
	if s.attachments == nil {
		return nil, status.Error(codes.Unavailable, "attachment storage is unavailable")
	}
	botID := strings.TrimSpace(req.GetBotId())
	contentHash := strings.TrimSpace(req.GetContentHash())
	containerPath := strings.TrimSpace(req.GetContainerPath())
	if botID == "" || (contentHash == "") == (containerPath == "") {
		return nil, status.Error(codes.InvalidArgument, "exactly one attachment source and bot id are required")
	}

	var (
		asset media.Asset
		err   error
	)
	if contentHash != "" {
		if !validContentHash(contentHash) {
			return nil, status.Error(codes.InvalidArgument, "invalid attachment content hash")
		}
		asset, err = s.attachments.Resolve(ctx, botID, contentHash)
	} else {
		cleanPath := filepath.Clean(containerPath)
		subpath, ok := attachment.DataSubpath(cleanPath)
		if !ok || strings.Contains(subpath, "..") {
			return nil, status.Error(codes.InvalidArgument, "workspace attachment path must be inside /data")
		}
		asset, err = s.attachments.IngestContainerFile(ctx, botID, cleanPath)
	}
	if err != nil {
		return nil, attachmentStatus(err)
	}
	if asset.SizeBytes > media.MaxAssetBytes {
		return nil, status.Error(codes.ResourceExhausted, "attachment exceeds the maximum size")
	}

	reader, _, err := s.attachments.Open(ctx, botID, asset.ContentHash)
	if err != nil {
		return nil, attachmentStatus(err)
	}
	md5Hash := md5.New()
	limited := &io.LimitedReader{R: reader, N: media.MaxAssetBytes + 1}
	size, copyErr := io.Copy(md5Hash, limited)
	closeErr := reader.Close()
	if copyErr != nil {
		return nil, attachmentStatus(copyErr)
	}
	if closeErr != nil {
		return nil, status.Error(codes.Internal, "failed to close attachment source")
	}
	if size == 0 {
		return nil, status.Error(codes.InvalidArgument, "attachment is empty")
	}
	if size > media.MaxAssetBytes {
		return nil, status.Error(codes.ResourceExhausted, "attachment exceeds the maximum size")
	}
	asset.RawMD5 = hex.EncodeToString(md5Hash.Sum(nil))
	asset.SizeBytes = size
	return &runtimepb.ResolveAttachmentResponse{
		BotId: asset.BotID, ContentHash: asset.ContentHash, Mime: asset.Mime,
		SizeBytes: size, StorageKey: asset.StorageKey, RawMd5: asset.RawMD5,
	}, nil
}

func (s *Server) ReadAttachment(req *runtimepb.ReadAttachmentRequest, stream runtimepb.RuntimeService_ReadAttachmentServer) error {
	if s.attachments == nil {
		return status.Error(codes.Unavailable, "attachment storage is unavailable")
	}
	botID := strings.TrimSpace(req.GetBotId())
	contentHash := strings.TrimSpace(req.GetContentHash())
	if botID == "" || !validContentHash(contentHash) {
		return status.Error(codes.InvalidArgument, "bot id and valid content hash are required")
	}
	reader, _, err := s.attachments.Open(stream.Context(), botID, contentHash)
	if err != nil {
		return attachmentStatus(err)
	}
	defer func() { _ = reader.Close() }()

	buffer := make([]byte, attachmentChunkSize)
	var sent int64
	for {
		n, readErr := reader.Read(buffer)
		if n > 0 {
			if sent+int64(n) > media.MaxAssetBytes {
				return status.Error(codes.ResourceExhausted, "attachment exceeds the maximum size")
			}
			chunk := append([]byte(nil), buffer[:n]...)
			if err := stream.Send(&runtimepb.AttachmentChunk{Data: chunk}); err != nil {
				return err
			}
			sent += int64(n)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return attachmentStatus(readErr)
		}
	}
}

func validContentHash(contentHash string) bool {
	if len(contentHash) != 64 {
		return false
	}
	_, err := hex.DecodeString(contentHash)
	return err == nil
}

func attachmentStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, media.ErrAssetNotFound):
		return status.Error(codes.NotFound, "attachment not found")
	case errors.Is(err, media.ErrAssetTooLarge):
		return status.Error(codes.ResourceExhausted, "attachment exceeds the maximum size")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "attachment transfer canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "attachment transfer deadline exceeded")
	default:
		return status.Error(codes.Internal, "attachment operation failed")
	}
}
