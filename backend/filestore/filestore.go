// Package filestore provides access to Anthropic Filestore filesystems.
package filestore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"path/filepath"
	"strings"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/filestore/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/pacer"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	backendName       = "rclone-filestore"
	defaultServiceURL = "http://localhost:9112"
	apiPathPrefix     = "v1/filestore/fs/"
	listPageSize      = 100
	mimeProbeSize     = 3072
	httpClientTimeout = 30 * time.Minute
	shortDeadline     = 30 * time.Second
	listDeadline      = 10 * time.Second
	minSleep          = 10 * time.Millisecond
	maxSleep          = 2 * time.Second
)

// Options defines the configuration for this backend.
type Options struct {
	URL          string `config:"url"`
	FilesystemID string `config:"filesystem_id"`
	AuthToken    string `config:"auth_token"`
}

// Fs represents a Filestore filesystem.
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	client   *http.Client
	pacer    *fs.Pacer
}

// Object describes a Filestore file.
type Object struct {
	fs          *Fs
	remote      string
	size        int64
	modTime     time.Time
	md5         string
	contentType string
	instanceID  string
}

type apiError struct {
	StatusCode  int
	ConnectCode string
	Message     string
}

type connectError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        backendName,
		Description: "Filestore Backend",
		NewFs:       NewFs,
		Options: []fs.Option{
			{
				Name:     "url",
				Help:     "Filestore service URL (handles file metadata + GCS operations internally)",
				Default:  defaultServiceURL,
				Required: true,
			}, {
				Name:     "filesystem_id",
				Help:     "Filesystem ID to use",
				Required: true,
			}, {
				Name:       "auth_token",
				Help:       "Authentication token (JWT)",
				IsPassword: true,
			},
		},
	})
}

func newHTTPTransport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// NewFs constructs a Filestore filesystem from the supplied root and config.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, fmt.Errorf("failed to set config: %w", err)
	}
	if !strings.HasSuffix(opt.URL, "/") {
		opt.URL += "/"
	}

	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
		client: &http.Client{
			Transport: newHTTPTransport(),
			Timeout:   httpClientTimeout,
		},
		pacer: fs.NewPacer(ctx, pacer.NewDefault(
			pacer.MinSleep(minSleep),
			pacer.MaxSleep(maxSleep),
		)),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)
	return f, nil
}

// Name returns the configured remote name.
func (f *Fs) Name() string { return f.name }

// Root returns the configured backend root.
func (f *Fs) Root() string { return f.root }

// String returns a description of the filesystem.
func (f *Fs) String() string { return backendName + ":" + f.root }

// Precision reports that modification times are not supported.
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes reports the MD5 returned by Filestore.
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.MD5) }

// Features returns the optional features implemented by the backend.
func (f *Fs) Features() *fs.Features { return f.features }

func (f *Fs) buildRequest(ctx context.Context, operation string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.opt.URL+apiPathPrefix+operation, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+f.opt.AuthToken)
	return req, nil
}

func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if err != nil {
		// Every transport error is retryable while the context remains live.
		_ = fserrors.ShouldRetry(err)
		return true, err
	}
	if resp != nil {
		switch {
		case resp.StatusCode == http.StatusRequestTimeout:
			return true, fmt.Errorf("request timeout: %d", resp.StatusCode)
		case resp.StatusCode == http.StatusTooManyRequests:
			return true, fmt.Errorf("rate limited: %d", resp.StatusCode)
		case resp.StatusCode >= 500 && resp.StatusCode <= 599:
			return true, fmt.Errorf("server error: %d", resp.StatusCode)
		}
	}
	return false, nil
}

func newAPIError(statusCode int, body []byte, structured bool) error {
	result := &apiError{StatusCode: statusCode, Message: string(body)}
	if structured {
		var connect connectError
		if json.Unmarshal(body, &connect) == nil && connect.Code != "" {
			result.ConnectCode = connect.Code
			result.Message = connect.Message
		}
	}
	return result
}

func (e *apiError) Error() string {
	if e.ConnectCode != "" {
		return fmt.Sprintf("API error %d (%s): %s", e.StatusCode, e.ConnectCode, e.Message)
	}
	return fmt.Sprintf("API error %d: %s", e.StatusCode, e.Message)
}

func apiErrorCode(err error, code string) bool {
	var target *apiError
	return errors.As(err, &target) && target.ConnectCode == code
}

func (f *Fs) callAPI(ctx context.Context, operation string, request proto.Message, timeout time.Duration) ([]byte, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	payload, err := protojson.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	var responseBody []byte
	err = f.pacer.Call(func() (bool, error) {
		req, err := f.buildRequest(ctx, operation, bytes.NewReader(payload))
		if err != nil {
			return false, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, requestErr := f.client.Do(req)
		var body []byte
		if resp != nil && resp.Body != nil {
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				return true, fmt.Errorf("failed to read response body: %w", err)
			}
		}
		if requestErr != nil {
			return shouldRetry(ctx, resp, requestErr)
		}

		if retry, retryErr := shouldRetry(ctx, resp, nil); retry {
			return true, retryErr
		}
		if resp.StatusCode >= 400 {
			return false, newAPIError(resp.StatusCode, body, true)
		}
		responseBody = body
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("API call failed: %w", err)
	}
	return responseBody, nil
}

func (f *Fs) callAPIStreaming(ctx context.Context, operation, contentType string, body io.Reader) ([]byte, error) {
	req, err := f.buildRequest(ctx, operation, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, responseBody)
	}
	return responseBody, nil
}

func unmarshalResponse(body []byte, response proto.Message) error {
	if err := protojson.Unmarshal(body, response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return nil
}

func apiPath(root, remote string) string {
	return filepath.Join(root, remote)
}

func timestampTime(value *timestamppb.Timestamp) time.Time {
	return value.AsTime()
}

func timestampMicrosecond(value *timestamppb.Timestamp) time.Time {
	return timestampTime(value).Truncate(time.Microsecond)
}

func newInstanceID() string {
	return uuid.NewString()
}

func (o *Object) setMetadata(file *api.File, truncateTime bool) {
	o.size = file.Size
	o.modTime = timestampTime(file.CreatedAt)
	if truncateTime {
		o.modTime = o.modTime.Truncate(time.Microsecond)
	}
	o.md5 = file.Md5
	o.contentType = file.MediaType
}

func (f *Fs) objectFromFile(remote string, wrapped *api.FilesystemFile, truncateTime bool) *Object {
	o := &Object{fs: f, remote: remote, instanceID: newInstanceID()}
	o.setMetadata(wrapped.File, truncateTime)
	return o
}

// List returns all immediate children of dir.
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	directoryPath := apiPath(f.root, dir)
	if directoryPath == "" {
		directoryPath = "/"
	}
	var entries fs.DirEntries
	cursor := ""
	for {
		request := &api.ListDirectoryRequest{
			FilesystemId: f.opt.FilesystemID,
			Path:         directoryPath,
			Limit:        listPageSize,
			Cursor:       cursor,
			Recursive:    false,
		}
		body, err := f.callAPI(ctx, "listDirectory", request, listDeadline)
		if err != nil {
			return nil, err
		}
		response := new(api.ListDirectoryResponse)
		if err := unmarshalResponse(body, response); err != nil {
			return nil, err
		}
		for _, entry := range response.Entries {
			switch item := entry.Entry.(type) {
			case *api.ListDirectoryResponse_Entry_File:
				if item.File == nil {
					continue
				}
				remote := filepath.Join(dir, filepath.Base(item.File.Path))
				o := f.objectFromFile(remote, item.File, true)
				fs.Debugf(o, "[Object-%s] Created from List for %s, modTime: %v", o.instanceID, o.remote, o.modTime)
				entries = append(entries, o)
			case *api.ListDirectoryResponse_Entry_Directory:
				if item.Directory == nil {
					continue
				}
				remote := filepath.Join(dir, filepath.Base(item.Directory.Path))
				entries = append(entries, fs.NewDir(remote, timestampMicrosecond(item.Directory.CreatedAt)))
			}
		}
		cursor = response.Cursor
		if cursor == "" {
			return entries, nil
		}
	}
}

// NewObject finds a file at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o := &Object{fs: f, remote: remote, instanceID: newInstanceID()}
	fs.Debugf(o, "[Object-%s] NewObject called for %s", o.instanceID, o.remote)
	if err := o.readMetaData(ctx); err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			fs.Debugf(o, "[Object-%s] NewObject: file not found %s", o.instanceID, o.remote)
		} else {
			fs.Debugf(o, "[Object-%s] NewObject failed to read metadata for %s: %v", o.instanceID, o.remote, err)
		}
		return nil, err
	}
	fs.Infof(o, "[Object-%s] NewObject successfully created for %s with cached metadata", o.instanceID, o.remote)
	return o, nil
}

// Mkdir creates dir and missing parents.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	directoryPath := apiPath(f.root, dir)
	if directoryPath == "" {
		directoryPath = "/"
	}
	request := &api.MakeDirectoryRequest{
		FilesystemId: f.opt.FilesystemID,
		Path:         directoryPath,
		MakeParents:  true,
	}
	_, err := f.callAPI(ctx, "makeDirectory", request, shortDeadline)
	return err
}

// Rmdir removes dir if it is empty.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	request := &api.RemoveDirectoryRequest{
		FilesystemId: f.opt.FilesystemID,
		Path:         apiPath(f.root, dir),
	}
	_, err := f.callAPI(ctx, "removeDirectory", request, shortDeadline)
	return err
}

func getMediaTypeWithReader(in io.Reader) (string, io.Reader, error) {
	buffered, ok := in.(*bufio.Reader)
	if !ok || buffered.Size() < mimeProbeSize+1024 {
		buffered = bufio.NewReaderSize(in, mimeProbeSize+1024)
	}
	prefix, err := buffered.Peek(mimeProbeSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", nil, fmt.Errorf("error peeking at magic bytes: %w", err)
	}
	mediaType, _, err := mime.ParseMediaType(mimetype.Detect(prefix).String())
	if err != nil {
		return "", nil, fmt.Errorf("error parsing media type: %w", err)
	}
	return mediaType, buffered, nil
}

// Put uploads or overwrites a file.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	requestCtx, cancel := context.WithTimeout(ctx, httpClientTimeout)
	defer cancel()
	mediaType, content, err := getMediaTypeWithReader(in)
	if err != nil {
		return nil, err
	}
	params := &api.CreateFileRequest_Params{
		FilesystemId:      f.opt.FilesystemID,
		Path:              apiPath(f.root, src.Remote()),
		MediaType:         mediaType,
		OverwriteExisting: true,
	}
	pipeReader, pipeWriter := io.Pipe()
	multipartWriter := multipart.NewWriter(pipeWriter)
	producerDone := make(chan error, 1)
	go writeMultipartUpload(requestCtx, multipartWriter, pipeWriter, params, content, producerDone)
	body, callErr := f.callAPIStreaming(requestCtx, "createFile", multipartWriter.FormDataContentType(), pipeReader)
	producerErr := <-producerDone
	if callErr != nil {
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("upload of %s timed out after %v: %w", params.Path, httpClientTimeout, callErr)
		}
		return nil, fmt.Errorf("upload of %s failed: %w", params.Path, callErr)
	}
	if producerErr != nil {
		return nil, fmt.Errorf("upload of %s failed during streaming: %w", params.Path, producerErr)
	}
	response := new(api.CreateFileResponse)
	if err := unmarshalResponse(body, response); err != nil {
		return nil, err
	}
	o := f.objectFromFile(src.Remote(), response.File, false)
	fs.Infof(o, "[Object-%s] Created from Put for %s, modTime: %v", o.instanceID, o.remote, o.modTime)
	return o, nil
}

func writeMultipartUpload(ctx context.Context, writer *multipart.Writer, pipe *io.PipeWriter, params proto.Message, in io.Reader, done chan<- error) {
	defer func() { _ = pipe.Close() }()
	finishWithError := func(err error) {
		_ = pipe.CloseWithError(err)
		done <- err
	}
	paramsJSON, err := protojson.Marshal(params)
	if err != nil {
		finishWithError(fmt.Errorf("failed to marshal params: %w", err))
		return
	}
	paramsHeader := make(textproto.MIMEHeader)
	paramsHeader.Set("Content-Disposition", `form-data; name="params"`)
	paramsHeader.Set("Content-Type", "application/json")
	paramsPart, err := writer.CreatePart(paramsHeader)
	if err != nil {
		finishWithError(fmt.Errorf("failed to create params field: %w", err))
		return
	}
	if _, err = paramsPart.Write(paramsJSON); err != nil {
		finishWithError(fmt.Errorf("failed to write params: %w", err))
		return
	}
	filePart, err := writer.CreateFormFile("file", "file")
	if err != nil {
		finishWithError(fmt.Errorf("failed to create file field: %w", err))
		return
	}
	if _, err = io.Copy(filePart, in); err != nil {
		var uploadErr error
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			uploadErr = fmt.Errorf("upload timeout after %v: %w", httpClientTimeout, err)
		case errors.Is(ctx.Err(), context.Canceled):
			uploadErr = fmt.Errorf("upload cancelled: %w", err)
		default:
			uploadErr = fmt.Errorf("failed to copy file data: %w", err)
		}
		finishWithError(uploadErr)
		return
	}
	if err := writer.Close(); err != nil {
		finishWithError(fmt.Errorf("failed to close multipart writer: %w", err))
		return
	}
	done <- nil
}

// Copy performs a server-side file copy.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	source, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	request := &api.CopyFileRequest{
		FilesystemId:      f.opt.FilesystemID,
		Source:            apiPath(f.root, source.remote),
		Destination:       apiPath(f.root, remote),
		OverwriteExisting: true,
	}
	body, err := f.callAPI(ctx, "copyFile", request, 0)
	if err != nil {
		return nil, err
	}
	response := new(api.CopyFileResponse)
	if err := unmarshalResponse(body, response); err != nil {
		return nil, err
	}
	o := f.objectFromFile(remote, response.File, false)
	fs.Infof(o, "[Object-%s] Created from Copy for %s, source was %s, modTime: %v", o.instanceID, o.remote, source.remote, o.modTime)
	return o, nil
}

// Move performs a server-side file move.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	source, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	request := &api.MoveFileRequest{
		FilesystemId:      f.opt.FilesystemID,
		Source:            apiPath(f.root, source.remote),
		Destination:       apiPath(f.root, remote),
		OverwriteExisting: true,
	}
	body, err := f.callAPI(ctx, "moveFile", request, shortDeadline)
	if err != nil {
		return nil, err
	}
	response := new(api.MoveFileResponse)
	if err := unmarshalResponse(body, response); err != nil {
		return nil, err
	}
	o := f.objectFromFile(remote, response.File, false)
	fs.Infof(o, "[Object-%s] Created from Move for %s, source was %s, modTime: %v", o.instanceID, o.remote, source.remote, o.modTime)
	return o, nil
}

// DirMove performs a server-side directory move.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	source, ok := src.(*Fs)
	if !ok {
		return fs.ErrorCantDirMove
	}
	request := &api.MoveDirectoryRequest{
		FilesystemId: f.opt.FilesystemID,
		Source:       apiPath(source.root, srcRemote),
		Destination:  apiPath(f.root, dstRemote),
	}
	_, err := f.callAPI(ctx, "moveDirectory", request, shortDeadline)
	return err
}

// Fs returns the filesystem which owns the object.
func (o *Object) Fs() fs.Info { return o.fs }

// String returns the remote path.
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path.
func (o *Object) Remote() string { return o.remote }

func (o *Object) ensureInstanceID() {
	if o.instanceID != "" {
		return
	}
	o.instanceID = newInstanceID()
	fs.Debugf(o, "[Object-%s] Instance ID generated for existing object %s", o.instanceID, o.remote)
}

// ModTime returns the cached creation time, reading metadata when necessary.
func (o *Object) ModTime(ctx context.Context) time.Time {
	o.ensureInstanceID()
	fs.Debugf(o, "[Object-%s] ModTime() called for %s", o.instanceID, o.remote)
	if err := o.readMetaData(ctx); err != nil {
		now := time.Now()
		fs.Logf(o, "[Object-%s] ERROR reading metadata for %s: %v, returning current time: %v", o.instanceID, o.remote, err, now)
		return now
	}
	modTime := o.modTime.Truncate(time.Microsecond)
	fs.Debugf(o, "[Object-%s] ModTime() returning cached time for %s: %v (raw: %v)", o.instanceID, o.remote, modTime, o.modTime)
	return modTime
}

// Size returns the object size in bytes.
func (o *Object) Size() int64 { return o.size }

// Storable reports that ordinary Filestore files are storable.
func (o *Object) Storable() bool { return true }

// Hash returns the Filestore MD5 hash when requested.
func (o *Object) Hash(_ context.Context, hashType hash.Type) (string, error) {
	if hashType != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.md5, nil
}

// SetModTime reports that Filestore does not provide a writable modification time.
func (o *Object) SetModTime(context.Context, time.Time) error { return fs.ErrorCantSetModTime }

// Open opens the object for full or ranged streaming reads.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	request := &api.ReadFileRequest{
		FilesystemId: o.fs.opt.FilesystemID,
		Path:         apiPath(o.fs.root, o.remote),
	}
	for _, option := range options {
		switch value := option.(type) {
		case *fs.SeekOption:
			request.Range = &api.ReadFileRequest_Range{Offset: value.Offset, Length: -1}
		case *fs.RangeOption:
			var offset, length int64
			if value.Start >= 0 {
				offset = value.Start
				if value.End >= 0 {
					length = value.End - value.Start + 1
				} else {
					length = -1
				}
			} else {
				if value.End >= 0 {
					offset = o.size - value.End
				}
				length = -1
			}
			request.Range = &api.ReadFileRequest_Range{Offset: offset, Length: length}
		}
	}
	payload, err := protojson.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	var response *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		req, err := o.fs.buildRequest(ctx, "readFile", bytes.NewReader(payload))
		if err != nil {
			return false, err
		}
		req.Header.Set("Content-Type", "application/json")
		response, err = o.fs.client.Do(req)
		if err != nil {
			retry, retryErr := shouldRetry(ctx, response, err)
			if response != nil && response.Body != nil {
				_, _ = io.ReadAll(response.Body)
				_ = response.Body.Close()
			}
			return retry, retryErr
		}
		if retry, retryErr := shouldRetry(ctx, response, nil); retry {
			_, _ = io.ReadAll(response.Body)
			_ = response.Body.Close()
			return true, retryErr
		}
		if response.StatusCode >= 400 {
			body, _ := io.ReadAll(response.Body)
			apiErr := newAPIError(response.StatusCode, body, true)
			_ = response.Body.Close()
			return false, apiErr
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

// Update overwrites the object and refreshes its metadata.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	updated, err := o.fs.Put(ctx, in, src, options...)
	if err != nil {
		return err
	}
	newObject := updated.(*Object)
	o.size = newObject.size
	o.modTime = newObject.modTime
	o.md5 = newObject.md5
	return nil
}

// Remove deletes the object.
func (o *Object) Remove(ctx context.Context) error {
	request := &api.RemoveFileRequest{
		FilesystemId: o.fs.opt.FilesystemID,
		Path:         apiPath(o.fs.root, o.remote),
	}
	_, err := o.fs.callAPI(ctx, "removeFile", request, shortDeadline)
	return err
}

func (o *Object) readMetaData(ctx context.Context) error {
	o.ensureInstanceID()
	if !o.modTime.IsZero() {
		fs.Debugf(o, "[Object-%s] readMetaData() CACHE HIT for %s, cached modTime: %v", o.instanceID, o.remote, o.modTime)
		return nil
	}
	fs.Debugf(o, "[Object-%s] readMetaData() CACHE MISS for %s, fetching from API", o.instanceID, o.remote)
	request := &api.ReadMetadataRequest{
		FilesystemId: o.fs.opt.FilesystemID,
		Path:         apiPath(o.fs.root, o.remote),
	}
	body, err := o.fs.callAPI(ctx, "readMetadata", request, shortDeadline)
	if err != nil {
		if apiErrorCode(err, "not_found") {
			return fs.ErrorObjectNotFound
		}
		return err
	}
	response := new(api.ReadMetadataResponse)
	if err := unmarshalResponse(body, response); err != nil {
		return err
	}
	file := response.GetFile()
	if file == nil || file.File == nil {
		return fs.ErrorObjectNotFound
	}
	oldModTime := o.modTime
	apiTimestamp := timestampTime(file.File.CreatedAt)
	o.setMetadata(file.File, true)
	fs.Infof(o, "[Object-%s] METADATA FETCHED for %s:\n  modTime: %v (was: %v)\n  size: %d\n  md5: %s\n  contentType: %s\n  API timestamp: %v", o.instanceID, o.remote, o.modTime, oldModTime, o.size, o.md5, o.contentType, apiTimestamp)
	return nil
}

var (
	_ fs.Fs       = (*Fs)(nil)
	_ fs.Copier   = (*Fs)(nil)
	_ fs.Mover    = (*Fs)(nil)
	_ fs.DirMover = (*Fs)(nil)
	_ fs.Object   = (*Object)(nil)
)
