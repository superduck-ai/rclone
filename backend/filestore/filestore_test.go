package filestore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/filestore/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testFilesystemID = "claude_chat_test"

func newTestFs(t *testing.T, serverURL, root, filesystemID, token string) *Fs {
	t.Helper()
	value, err := NewFs(context.Background(), "test", root, configmap.Simple{
		"url":           serverURL,
		"filesystem_id": filesystemID,
		"auth_token":    token,
	})
	require.NoError(t, err)
	return value.(*Fs)
}

func assertCommonRequest(t *testing.T, request *http.Request, operation, token string) {
	t.Helper()
	assert.Equal(t, http.MethodPost, request.Method)
	assert.Equal(t, "/v1/filestore/fs/"+operation, request.URL.Path)
	assert.Equal(t, "Bearer "+token, request.Header.Get("Authorization"))
}

func writeJSON(t *testing.T, response http.ResponseWriter, value string) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json")
	_, err := io.WriteString(response, value)
	require.NoError(t, err)
}

func decodeRequestJSON(t *testing.T, request *http.Request) map[string]any {
	t.Helper()
	body, err := io.ReadAll(request.Body)
	require.NoError(t, err)
	var value map[string]any
	require.NoError(t, json.Unmarshal(body, &value), string(body))
	return value
}

func metadataResponse(path, createdAt, md5 string, size int) string {
	return `{"file":{"file":{"createdAt":"` + createdAt + `","size":"` +
		strconv.Itoa(size) + `","mediaType":"text/plain","md5":"` + md5 +
		`"},"filesystemId":"` + testFilesystemID + `","path":"` + path + `"}}`
}

func TestNewFsMatchesRecoveredConfiguration(t *testing.T) {
	value, err := NewFs(context.Background(), "slug", "reports/../raw", configmap.Simple{
		"url":           "HTTP://User@Example.COM/base//?x=1",
		"filesystem_id": "  id  ",
		"auth_token":    "  token  ",
	})
	require.NoError(t, err)
	f := value.(*Fs)
	assert.Equal(t, "HTTP://User@Example.COM/base//?x=1/", f.opt.URL)
	assert.Equal(t, "  id  ", f.opt.FilesystemID)
	assert.Equal(t, "  token  ", f.opt.AuthToken)
	assert.Equal(t, "reports/../raw", f.Root())
	assert.Equal(t, "rclone-filestore:reports/../raw", f.String())
	assert.Equal(t, fs.ModTimeNotSupported, f.Precision())
	assert.True(t, f.Features().CanHaveEmptyDirectories)
	assert.False(t, f.Features().ReadMimeType)
	assert.Nil(t, f.Features().PutStream)

	transport, ok := f.client.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Equal(t, 20, transport.MaxIdleConns)
	assert.Equal(t, 90*time.Second, transport.IdleConnTimeout)
	assert.False(t, transport.ForceAttemptHTTP2)
	require.NotNil(t, transport.Protocols)
	assert.True(t, transport.Protocols.HTTP1())
	assert.False(t, transport.Protocols.HTTP2())
	assert.Equal(t, 30*time.Minute, f.client.Timeout)
	assert.Nil(t, f.client.CheckRedirect)

	registered, err := fs.Find(backendName)
	require.NoError(t, err)
	assert.Equal(t, "Filestore Backend", registered.Description)
	options := map[string]fs.Option{}
	for _, option := range registered.Options {
		options[option.Name] = option
	}
	assert.Equal(t, defaultServiceURL, options["url"].Default)
	assert.True(t, options["url"].Required)
	assert.Equal(t, "Filestore service URL (handles file metadata + GCS operations internally)", options["url"].Help)
	assert.True(t, options["filesystem_id"].Required)
	assert.Equal(t, "Filesystem ID to use", options["filesystem_id"].Help)
	assert.False(t, options["auth_token"].Required)
	assert.True(t, options["auth_token"].IsPassword)
	assert.False(t, options["auth_token"].Sensitive)
	assert.Equal(t, "Authentication token (JWT)", options["auth_token"].Help)
	_, hasAllowInsecure := options["allow_insecure"]
	assert.False(t, hasAllowInsecure)
}

func TestNewFsPreservesExistingTrailingSlashes(t *testing.T) {
	f := newTestFs(t, "https://example.invalid/base///", "/", "id", "token")
	assert.Equal(t, "https://example.invalid/base///", f.opt.URL)
}

func TestListWirePaginationAndRecoveredEntryMapping(t *testing.T) {
	const createdAt = "2026-07-21T03:04:05.123456789Z"
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		assertCommonRequest(t, request, "listDirectory", "token")
		payload := decodeRequestJSON(t, request)
		assert.Equal(t, "/outputs/dir", payload["path"])
		assert.Equal(t, "100", payload["limit"])
		call := calls.Add(1)
		switch call {
		case 1:
			assert.NotContains(t, payload, "cursor")
			writeJSON(t, response, `{"entries":[`+
				`{"file":{"file":{"createdAt":"`+createdAt+`","size":"3","mediaType":"text/plain","md5":"RAW-MD5"},"path":"/untrusted/parent/file.txt"}},`+
				`{"directory":{"path":"/elsewhere/sub","createdAt":"`+createdAt+`"}},`+
				`{}],"cursor":"repeat"}`)
		case 2:
			assert.Equal(t, "repeat", payload["cursor"])
			writeJSON(t, response, `{"cursor":"repeat"}`)
		case 3:
			assert.Equal(t, "repeat", payload["cursor"])
			writeJSON(t, response, `{}`)
		default:
			t.Fatalf("unexpected list call %d", call)
		}
	}))
	defer server.Close()

	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
	entries, err := f.List(context.Background(), "dir")
	require.NoError(t, err)
	assert.Equal(t, int32(3), calls.Load())
	require.Len(t, entries, 2)
	file := entries[0].(*Object)
	assert.Equal(t, "dir/file.txt", file.remote)
	assert.Equal(t, "RAW-MD5", file.md5)
	assert.Equal(t, time.Date(2026, 7, 21, 3, 4, 5, 123456000, time.UTC), file.modTime)
	_, err = uuid.Parse(file.instanceID)
	assert.NoError(t, err)
	assert.Equal(t, "dir/sub", entries[1].Remote())
	assert.Equal(t, file.modTime, entries[1].ModTime(context.Background()))
}

func TestListPanicsForFileWrapperWithoutNestedFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(t, response, `{"entries":[{"file":{"path":"/outputs/file"}}]}`)
	}))
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")

	assert.Panics(t, func() {
		_, _ = f.List(context.Background(), "")
	})
}

func TestNewObjectMetadataCachingAndModTimeFallback(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		assertCommonRequest(t, request, "readMetadata", "token")
		calls.Add(1)
		writeJSON(t, response, metadataResponse("/outputs/file", "2026-07-21T03:04:05.987654321Z", "ABCDEF", 7))
	}))
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")

	value, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)
	o := value.(*Object)
	assert.Equal(t, "ABCDEF", o.md5)
	assert.Equal(t, time.Date(2026, 7, 21, 3, 4, 5, 987654000, time.UTC), o.ModTime(context.Background()))
	assert.Equal(t, int32(1), calls.Load(), "cached ModTime must not read metadata again")

	lazy := &Object{fs: f, remote: "file"}
	assert.Equal(t, o.modTime, lazy.ModTime(context.Background()))
	assert.NotEmpty(t, lazy.instanceID)
	assert.Equal(t, int32(2), calls.Load())

	failingServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusUnauthorized)
		writeJSON(t, response, `{"code":"unauthenticated","message":"bad token"}`)
	}))
	defer failingServer.Close()
	failing := newTestFs(t, failingServer.URL, "/outputs", testFilesystemID, "token")
	failed := &Object{fs: failing, remote: "file"}
	before := time.Now()
	got := failed.ModTime(context.Background())
	after := time.Now()
	assert.False(t, got.Before(before))
	assert.False(t, got.After(after))
}

func TestMissingCreatedAtUsesProtobufEpochAndCachesIt(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeJSON(t, response, `{"file":{"file":{"size":"1"},"path":"/outputs/file"}}`)
	}))
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")

	value, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)
	o := value.(*Object)
	assert.Equal(t, time.Unix(0, 0).UTC(), o.modTime)
	assert.Equal(t, o.modTime, o.ModTime(context.Background()))
	assert.Equal(t, int32(1), calls.Load())
}

func TestCopyMoveAndDirMoveUseRecoveredRoots(t *testing.T) {
	var operations []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		operation := strings.TrimPrefix(request.URL.Path, "/v1/filestore/fs/")
		operations = append(operations, operation)
		payload := decodeRequestJSON(t, request)
		assert.Equal(t, testFilesystemID, payload["filesystemId"])
		switch operation {
		case "copyFile", "moveFile":
			assert.Equal(t, "/target/a.txt", payload["source"])
			assert.Equal(t, "/target/b.txt", payload["destination"])
			writeJSON(t, response, metadataResponse("/target/b.txt", "2026-07-21T03:04:05.123456789Z", "md5", 1))
		case "moveDirectory":
			assert.Equal(t, "/source/source-dir", payload["source"])
			assert.Equal(t, "/target/destination-dir", payload["destination"])
			writeJSON(t, response, `{}`)
		default:
			t.Fatalf("unexpected operation %q", operation)
		}
	}))
	defer server.Close()

	sourceServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("source service must not receive server-side operation")
	}))
	defer sourceServer.Close()
	sourceFs := newTestFs(t, sourceServer.URL, "/source", "different_filesystem", "source-token")
	targetFs := newTestFs(t, server.URL, "/target", testFilesystemID, "target-token")
	sourceObject := &Object{fs: sourceFs, remote: "a.txt", size: 1}

	copied, err := targetFs.Copy(context.Background(), sourceObject, "b.txt")
	require.NoError(t, err)
	moved, err := targetFs.Move(context.Background(), sourceObject, "b.txt")
	require.NoError(t, err)
	rawTime := time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.UTC)
	assert.Equal(t, rawTime, copied.(*Object).modTime)
	assert.Equal(t, rawTime, moved.(*Object).modTime)
	assert.Equal(t, rawTime.Truncate(time.Microsecond), copied.ModTime(context.Background()))
	assert.Equal(t, rawTime.Truncate(time.Microsecond), moved.ModTime(context.Background()))
	require.NoError(t, targetFs.DirMove(context.Background(), sourceFs, "source-dir", "destination-dir"))
	assert.Equal(t, []string{"copyFile", "moveFile", "moveDirectory"}, operations)

	_, err = targetFs.Copy(context.Background(), nil, "b.txt")
	assert.ErrorIs(t, err, fs.ErrorCantCopy)
	_, err = targetFs.Move(context.Background(), nil, "b.txt")
	assert.ErrorIs(t, err, fs.ErrorCantMove)
}

func TestMutationAndNotFoundSemantics(t *testing.T) {
	var operations []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		operation := strings.TrimPrefix(request.URL.Path, "/v1/filestore/fs/")
		operations = append(operations, operation)
		payload := decodeRequestJSON(t, request)
		switch operation {
		case "makeDirectory":
			assert.Equal(t, "/outputs/a", payload["path"])
			assert.Equal(t, true, payload["makeParents"])
			writeJSON(t, response, `{}`)
		case "removeDirectory", "removeFile", "listDirectory":
			response.WriteHeader(http.StatusNotFound)
			writeJSON(t, response, `{"code":"not_found","message":"missing"}`)
		case "readMetadata":
			response.WriteHeader(http.StatusNotFound)
			writeJSON(t, response, `{"code":"not_found","message":"missing"}`)
		default:
			t.Fatalf("unexpected operation %q", operation)
		}
	}))
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")

	require.NoError(t, f.Mkdir(context.Background(), "a"))
	err := f.Rmdir(context.Background(), "missing")
	assertAPIErrorCode(t, err, "not_found")
	assert.NotErrorIs(t, err, fs.ErrorDirNotFound)
	err = (&Object{fs: f, remote: "missing"}).Remove(context.Background())
	assertAPIErrorCode(t, err, "not_found")
	assert.NotErrorIs(t, err, fs.ErrorObjectNotFound)
	_, err = f.List(context.Background(), "missing")
	assertAPIErrorCode(t, err, "not_found")
	assert.NotErrorIs(t, err, fs.ErrorDirNotFound)
	_, err = f.NewObject(context.Background(), "missing")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	assert.Equal(t, []string{"makeDirectory", "removeDirectory", "removeFile", "listDirectory", "readMetadata"}, operations)
}

func assertAPIErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var target *apiError
	require.ErrorAs(t, err, &target)
	assert.Equal(t, code, target.ConnectCode)
}

func TestPutMultipartAndRawError(t *testing.T) {
	const content = "hello filestore"
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		assertCommonRequest(t, request, "createFile", "token")
		calls.Add(1)
		require.NoError(t, request.ParseMultipartForm(1<<20))
		var params map[string]any
		require.NoError(t, json.Unmarshal([]byte(request.FormValue("params")), &params))
		assert.Equal(t, testFilesystemID, params["filesystemId"])
		assert.Equal(t, "/outputs/file.txt", params["path"])
		assert.Equal(t, "text/plain", params["mediaType"])
		assert.Equal(t, true, params["overwriteExisting"])
		file, header, err := request.FormFile("file")
		require.NoError(t, err)
		defer func() { require.NoError(t, file.Close()) }()
		assert.Equal(t, "file", header.Filename)
		data, err := io.ReadAll(file)
		require.NoError(t, err)
		assert.Equal(t, content, string(data))
		writeJSON(t, response, metadataResponse("/outputs/file.txt", "2026-07-21T03:04:05.123456789Z", "RAW", len(content)))
	}))
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
	src := object.NewStaticObjectInfo("file.txt", time.Now(), int64(len(content)), true, nil, f)
	value, err := f.Put(context.Background(), strings.NewReader(content), src)
	require.NoError(t, err)
	o := value.(*Object)
	assert.Equal(t, "RAW", o.md5)
	assert.NotEmpty(t, o.instanceID)
	rawTime := time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.UTC)
	assert.Equal(t, rawTime, o.modTime)
	assert.Equal(t, rawTime.Truncate(time.Microsecond), o.ModTime(context.Background()))
	assert.Equal(t, int32(1), calls.Load())

	errorCalls := atomic.Int32{}
	errorServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		errorCalls.Add(1)
		response.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(t, response, `{"code":"not_found","message":"raw upload error"}`)
	}))
	defer errorServer.Close()
	errorFs := newTestFs(t, errorServer.URL, "/outputs", testFilesystemID, "token")
	_, err = errorFs.Put(context.Background(), strings.NewReader(content), src)
	require.EqualError(t, err, `upload of /outputs/file.txt failed: API error 503: {"code":"not_found","message":"raw upload error"}`)
	assert.Equal(t, int32(1), errorCalls.Load())
}

func TestPutStreamsChunkedOverHTTP1WhenServerOffersHTTP2(t *testing.T) {
	const content = "hello filestore"
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		assert.Equal(t, "HTTP/1.1", request.Proto)
		assert.Equal(t, int64(-1), request.ContentLength)
		assert.Equal(t, []string{"chunked"}, request.TransferEncoding)
		require.NoError(t, request.ParseMultipartForm(1<<20))
		assert.NotEmpty(t, request.FormValue("params"))
		writeJSON(t, response, metadataResponse("/outputs/file.txt", "2026-07-21T03:04:05Z", "RAW", len(content)))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
	transport := f.client.Transport.(*http.Transport)
	transport.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	src := object.NewStaticObjectInfo("file.txt", time.Now(), int64(len(content)), true, nil, f)
	_, err := f.Put(context.Background(), strings.NewReader(content), src)
	require.NoError(t, err)
}

func TestUpdateOnlyCopiesRecoveredMetadataFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		require.NoError(t, request.ParseMultipartForm(1<<20))
		writeJSON(t, response, metadataResponse("/outputs/file.txt", "2026-07-21T03:04:05.123456789Z", "new-md5", 11))
	}))
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
	o := &Object{
		fs:          f,
		remote:      "file.txt",
		size:        3,
		modTime:     time.Unix(1, 0),
		md5:         "old-md5",
		contentType: "application/old",
		instanceID:  "existing-instance",
	}
	src := object.NewStaticObjectInfo("file.txt", time.Now(), 11, true, nil, f)

	require.NoError(t, o.Update(context.Background(), strings.NewReader("new content"), src))
	assert.Equal(t, int64(11), o.size)
	assert.Equal(t, "new-md5", o.md5)
	assert.Equal(t, time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.UTC), o.modTime)
	assert.Equal(t, "application/old", o.contentType)
	assert.Equal(t, "existing-instance", o.instanceID)
}

func TestMediaTypeProbeReusesLargeBufferedReader(t *testing.T) {
	const content = "plain text remains available after detection"
	buffered := bufio.NewReaderSize(strings.NewReader(content), mimeProbeSize+1024)
	mediaType, reader, err := getMediaTypeWithReader(buffered)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", mediaType)
	assert.Same(t, buffered, reader)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, string(got))

	small := bufio.NewReaderSize(strings.NewReader(content), 1024)
	_, reader, err = getMediaTypeWithReader(small)
	require.NoError(t, err)
	assert.NotSame(t, small, reader)
}

func TestOpenRangeMapping(t *testing.T) {
	tests := []struct {
		name    string
		options []fs.OpenOption
		offset  any
		length  any
	}{
		{name: "seek", options: []fs.OpenOption{&fs.SeekOption{Offset: 3}}, offset: "3", length: "-1"},
		{name: "negative seek", options: []fs.OpenOption{&fs.SeekOption{Offset: -3}}, offset: "-3", length: "-1"},
		{name: "bounded", options: []fs.OpenOption{&fs.RangeOption{Start: 2, End: 5}}, offset: "2", length: "4"},
		{name: "open ended", options: []fs.OpenOption{&fs.RangeOption{Start: 2, End: -1}}, offset: "2", length: "-1"},
		{name: "suffix", options: []fs.OpenOption{&fs.RangeOption{Start: -1, End: 4}}, offset: "6", length: "-1"},
		{name: "oversized suffix", options: []fs.OpenOption{&fs.RangeOption{Start: -1, End: 15}}, offset: "-5", length: "-1"},
		{name: "both negative", options: []fs.OpenOption{&fs.RangeOption{Start: -1, End: -1}}, offset: nil, length: "-1"},
		{name: "last wins", options: []fs.OpenOption{&fs.SeekOption{Offset: 1}, &fs.RangeOption{Start: 4, End: 5}}, offset: "4", length: "2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				payload := decodeRequestJSON(t, request)
				rangeValue := payload["range"].(map[string]any)
				assert.Equal(t, test.offset, rangeValue["offset"])
				assert.Equal(t, test.length, rangeValue["length"])
				_, _ = io.WriteString(response, "body")
			}))
			defer server.Close()
			f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
			o := &Object{fs: f, remote: "file", size: 10}
			reader, err := o.Open(context.Background(), test.options...)
			require.NoError(t, err)
			_, err = io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
		})
	}
}

func TestDefaultRedirectAndStatusBelow400(t *testing.T) {
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		assert.Equal(t, http.MethodPost, request.Method)
		assert.Equal(t, "Bearer token", request.Header.Get("Authorization"))
		writeJSON(t, response, metadataResponse("/outputs/file", "2026-07-21T03:04:05Z", "md5", 1))
	}))
	defer redirectTarget.Close()
	redirectSource := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Location", redirectTarget.URL+request.URL.Path)
		response.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirectSource.Close()
	f := newTestFs(t, redirectSource.URL, "/outputs", testFilesystemID, "token")
	_, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)

	statusServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusMultipleChoices)
		writeJSON(t, response, metadataResponse("/outputs/file", "2026-07-21T03:04:05Z", "md5", 1))
	}))
	defer statusServer.Close()
	statusFs := newTestFs(t, statusServer.URL, "/outputs", testFilesystemID, "token")
	_, err = statusFs.NewObject(context.Background(), "file")
	require.NoError(t, err)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type errorReadCloser struct {
	err error
}

func (r errorReadCloser) Read([]byte) (int, error) { return 0, r.err }
func (errorReadCloser) Close() error               { return nil }

func TestCallAPIReadsResponseBeforeRetryClassification(t *testing.T) {
	f := newTestFs(t, "http://example.invalid", "/outputs", testFilesystemID, "token")
	f.pacer.SetRetries(1)
	readErr := errors.New("broken response stream")
	f.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusUnauthorized,
			Header:     make(http.Header),
			Body:       errorReadCloser{err: readErr},
			Request:    request,
		}, nil
	})

	_, err := f.callAPI(context.Background(), "readMetadata", &api.ReadMetadataRequest{}, time.Second)
	require.EqualError(t, err, "API call failed: failed to read response body: broken response stream")
}

func TestRetrySemantics(t *testing.T) {
	t.Run("retry statuses", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) < 3 {
				response.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeJSON(t, response, metadataResponse("/outputs/file", "2026-07-21T03:04:05Z", "md5", 1))
		}))
		defer server.Close()
		f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
		_, err := f.NewObject(context.Background(), "file")
		require.NoError(t, err)
		assert.Equal(t, int32(3), calls.Load())
	})

	t.Run("retry every live transport error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			writeJSON(t, response, metadataResponse("/outputs/file", "2026-07-21T03:04:05Z", "md5", 1))
		}))
		defer server.Close()
		f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
		base := f.client.Transport
		var calls atomic.Int32
		sentinel := errors.New("permanent transport error")
		assert.False(t, fserrors.ShouldRetry(sentinel))
		f.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				return nil, sentinel
			}
			return base.RoundTrip(request)
		})
		_, err := f.NewObject(context.Background(), "file")
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("canceled context stops", func(t *testing.T) {
		f := newTestFs(t, "http://127.0.0.1:1", "/outputs", testFilesystemID, "token")
		var calls atomic.Int32
		f.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, request.Context().Err()
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := f.NewObject(ctx, "file")
		require.Error(t, err)
		assert.LessOrEqual(t, calls.Load(), int32(1))
	})
}

func TestRetryStatusErrorsMatchRecoveredClient(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{status: http.StatusRequestTimeout, want: "request timeout: 408"},
		{status: http.StatusTooManyRequests, want: "rate limited: 429"},
		{status: http.StatusInternalServerError, want: "server error: 500"},
	}
	for _, test := range tests {
		response := &http.Response{StatusCode: test.status}
		retry, err := shouldRetry(context.Background(), response, nil)
		assert.True(t, retry)
		require.EqualError(t, err, test.want)
		assert.False(t, fserrors.ShouldRetry(err), "status errors are ordinary fmt errors")
	}
}

func TestObjectHashAndInterfaces(t *testing.T) {
	o := &Object{md5: "UPPER-or-invalid"}
	got, err := o.Hash(context.Background(), hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "UPPER-or-invalid", got)
	_, err = o.Hash(context.Background(), hash.SHA1)
	assert.ErrorIs(t, err, hash.ErrUnsupported)
	_, isMimeTyper := any(o).(fs.MimeTyper)
	assert.False(t, isMimeTyper)
	_, isPutStreamer := any(&Fs{}).(fs.PutStreamer)
	assert.False(t, isPutStreamer)
}
