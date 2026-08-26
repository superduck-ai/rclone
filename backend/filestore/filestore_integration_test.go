package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memoryFile struct {
	data      []byte
	mediaType string
}

type memoryFilestore struct {
	t          *testing.T
	mu         sync.Mutex
	filesystem string
	dirs       map[string]bool
	files      map[string]memoryFile
}

func newMemoryFilestore(t *testing.T) *memoryFilestore {
	return &memoryFilestore{
		t:          t,
		filesystem: testFilesystemID,
		dirs:       map[string]bool{"/": true, "/outputs": true},
		files:      map[string]memoryFile{},
	}
}

func (store *memoryFilestore) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	store.mu.Lock()
	defer store.mu.Unlock()
	operation := strings.TrimPrefix(request.URL.Path, "/v1/filestore/fs/")
	if request.Method != http.MethodPost || operation == request.URL.Path {
		response.WriteHeader(http.StatusNotFound)
		return
	}
	if operation == "createFile" {
		store.createFile(response, request)
		return
	}
	payload := decodeRequestJSON(store.t, request)
	if payload["filesystemId"] != store.filesystem {
		store.apiError(response, http.StatusForbidden, "denied", "wrong filesystem")
		return
	}
	switch operation {
	case "listDirectory":
		store.listDirectory(response, payload)
	case "makeDirectory":
		store.makeDirectory(response, payload)
	case "removeDirectory":
		store.removeDirectory(response, payload)
	case "copyFile":
		store.copyFile(response, payload, false)
	case "moveFile":
		store.copyFile(response, payload, true)
	case "moveDirectory":
		store.moveDirectory(response, payload)
	case "readFile":
		store.readFile(response, payload)
	case "removeFile":
		store.removeFile(response, payload)
	case "readMetadata":
		store.readMetadata(response, payload)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
}

func (store *memoryFilestore) apiError(response http.ResponseWriter, status int, code, message string) {
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]string{"code": code, "message": message})
}

func stringField(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func (store *memoryFilestore) ensureParents(value string) {
	for current := path.Dir(value); current != "." && current != "/"; current = path.Dir(current) {
		store.dirs[current] = true
	}
	store.dirs["/"] = true
}

func (store *memoryFilestore) fileEnvelope(filePath string, file memoryFile) map[string]any {
	return map[string]any{
		"file": map[string]any{
			"createdAt": "2026-07-21T03:04:05Z",
			"size":      strconv.FormatInt(int64(len(file.data)), 10),
			"mediaType": file.mediaType,
		},
		"filesystemId": store.filesystem,
		"path":         filePath,
	}
}

func (store *memoryFilestore) directoryEnvelope(directoryPath string) map[string]any {
	return map[string]any{
		"filesystemId": store.filesystem,
		"path":         directoryPath,
		"createdAt":    "2026-07-21T03:04:05Z",
	}
}

func (store *memoryFilestore) listDirectory(response http.ResponseWriter, payload map[string]any) {
	directory := stringField(payload, "path")
	if !store.dirs[directory] {
		store.apiError(response, http.StatusNotFound, "not_found", "directory missing")
		return
	}
	type namedEntry struct {
		name  string
		value map[string]any
	}
	var entries []namedEntry
	for filePath, file := range store.files {
		if path.Dir(filePath) == directory {
			entries = append(entries, namedEntry{name: filePath, value: map[string]any{"file": store.fileEnvelope(filePath, file)}})
		}
	}
	for directoryPath := range store.dirs {
		if directoryPath != directory && path.Dir(directoryPath) == directory {
			entries = append(entries, namedEntry{name: directoryPath, value: map[string]any{"directory": store.directoryEnvelope(directoryPath)}})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	values := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		values = append(values, entry.value)
	}
	_ = json.NewEncoder(response).Encode(map[string]any{"entries": values})
}

func (store *memoryFilestore) makeDirectory(response http.ResponseWriter, payload map[string]any) {
	directory := stringField(payload, "path")
	store.ensureParents(directory)
	store.dirs[directory] = true
	_, _ = io.WriteString(response, `{}`)
}

func (store *memoryFilestore) removeDirectory(response http.ResponseWriter, payload map[string]any) {
	directory := stringField(payload, "path")
	if !store.dirs[directory] {
		store.apiError(response, http.StatusNotFound, "not_found", "directory missing")
		return
	}
	for filePath := range store.files {
		if path.Dir(filePath) == directory {
			store.apiError(response, http.StatusConflict, "directory_not_empty", "directory not empty")
			return
		}
	}
	for child := range store.dirs {
		if child != directory && path.Dir(child) == directory {
			store.apiError(response, http.StatusConflict, "directory_not_empty", "directory not empty")
			return
		}
	}
	delete(store.dirs, directory)
	_, _ = io.WriteString(response, `{}`)
}

func (store *memoryFilestore) createFile(response http.ResponseWriter, request *http.Request) {
	require.NoError(store.t, request.ParseMultipartForm(1<<20))
	var params map[string]any
	require.NoError(store.t, json.Unmarshal([]byte(request.FormValue("params")), &params))
	if params["filesystemId"] != store.filesystem {
		store.apiError(response, http.StatusForbidden, "denied", "wrong filesystem")
		return
	}
	filePart, _, err := request.FormFile("file")
	require.NoError(store.t, err)
	defer func() { require.NoError(store.t, filePart.Close()) }()
	data, err := io.ReadAll(filePart)
	require.NoError(store.t, err)
	filePath := stringField(params, "path")
	file := memoryFile{data: data, mediaType: stringField(params, "mediaType")}
	store.ensureParents(filePath)
	store.files[filePath] = file
	_ = json.NewEncoder(response).Encode(map[string]any{"file": store.fileEnvelope(filePath, file)})
}

func (store *memoryFilestore) copyFile(response http.ResponseWriter, payload map[string]any, move bool) {
	source := stringField(payload, "source")
	destination := stringField(payload, "destination")
	file, found := store.files[source]
	if !found {
		store.apiError(response, http.StatusNotFound, "not_found", "file missing")
		return
	}
	file.data = bytes.Clone(file.data)
	store.ensureParents(destination)
	store.files[destination] = file
	if move {
		delete(store.files, source)
	}
	_ = json.NewEncoder(response).Encode(map[string]any{"file": store.fileEnvelope(destination, file)})
}

func (store *memoryFilestore) moveDirectory(response http.ResponseWriter, payload map[string]any) {
	source := stringField(payload, "source")
	destination := stringField(payload, "destination")
	if !store.dirs[source] {
		store.apiError(response, http.StatusNotFound, "not_found", "directory missing")
		return
	}
	store.ensureParents(destination)
	for directory := range store.dirs {
		if directory == source || strings.HasPrefix(directory, source+"/") {
			delete(store.dirs, directory)
			store.dirs[destination+strings.TrimPrefix(directory, source)] = true
		}
	}
	for filePath, file := range store.files {
		if strings.HasPrefix(filePath, source+"/") {
			delete(store.files, filePath)
			store.files[destination+strings.TrimPrefix(filePath, source)] = file
		}
	}
	_, _ = io.WriteString(response, `{}`)
}

func (store *memoryFilestore) readFile(response http.ResponseWriter, payload map[string]any) {
	file, found := store.files[stringField(payload, "path")]
	if !found {
		store.apiError(response, http.StatusNotFound, "not_found", "file missing")
		return
	}
	data := file.data
	if rangeValue, ok := payload["range"].(map[string]any); ok {
		offset, _ := strconv.ParseInt(stringField(rangeValue, "offset"), 10, 64)
		length, _ := strconv.ParseInt(stringField(rangeValue, "length"), 10, 64)
		if offset > int64(len(data)) {
			offset = int64(len(data))
		}
		end := int64(len(data))
		if length >= 0 && offset+length < end {
			end = offset + length
		}
		data = data[offset:end]
	}
	_, _ = response.Write(data)
}

func (store *memoryFilestore) removeFile(response http.ResponseWriter, payload map[string]any) {
	filePath := stringField(payload, "path")
	if _, found := store.files[filePath]; !found {
		store.apiError(response, http.StatusNotFound, "not_found", "file missing")
		return
	}
	delete(store.files, filePath)
	_, _ = io.WriteString(response, `{}`)
}

func (store *memoryFilestore) readMetadata(response http.ResponseWriter, payload map[string]any) {
	value := stringField(payload, "path")
	if file, found := store.files[value]; found {
		_ = json.NewEncoder(response).Encode(map[string]any{"file": store.fileEnvelope(value, file)})
		return
	}
	if store.dirs[value] {
		_ = json.NewEncoder(response).Encode(map[string]any{"directory": store.directoryEnvelope(value)})
		return
	}
	store.apiError(response, http.StatusNotFound, "not_found", "metadata missing")
}

func TestBackendStatefulOperations(t *testing.T) {
	store := newMemoryFilestore(t)
	server := httptest.NewServer(store)
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
	ctx := context.Background()

	require.NoError(t, f.Mkdir(ctx, "work/nested"))
	entries, err := f.List(ctx, "work")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "work/nested", entries[0].Remote())

	content := []byte("0123456789")
	src := object.NewStaticObjectInfo("work/nested/data.bin", time.Now(), int64(len(content)), true, nil, f)
	uploaded, err := f.Put(ctx, bytes.NewReader(content), src)
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), uploaded.Size())

	reader, err := uploaded.Open(ctx, &fs.RangeOption{Start: 3, End: 6})
	require.NoError(t, err)
	rangeData, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "3456", string(rangeData))

	copied, err := f.Copy(ctx, uploaded, "work/nested/copy.bin")
	require.NoError(t, err)
	moved, err := f.Move(ctx, copied, "work/nested/moved.bin")
	require.NoError(t, err)
	assert.Equal(t, "work/nested/moved.bin", moved.Remote())
	_, err = f.NewObject(ctx, "work/nested/copy.bin")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)

	require.NoError(t, f.DirMove(ctx, f, "work/nested", "archive"))
	moved, err = f.NewObject(ctx, "archive/moved.bin")
	require.NoError(t, err)
	require.NoError(t, moved.Update(ctx, strings.NewReader("updated"), object.NewStaticObjectInfo("archive/moved.bin", time.Now(), 7, true, nil, f)))
	reader, err = moved.Open(ctx)
	require.NoError(t, err)
	updated, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "updated", string(updated))

	require.NoError(t, moved.Remove(ctx))
	original, err := f.NewObject(ctx, "archive/data.bin")
	require.NoError(t, err)
	require.NoError(t, original.Remove(ctx))
	require.NoError(t, f.Rmdir(ctx, "archive"))
	require.NoError(t, f.Rmdir(ctx, "work"))
	entries, err = f.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, entries)

	_, err = f.NewObject(ctx, "archive/data.bin")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

func TestBackendEmptyUnknownLengthAndSpecialPathUploads(t *testing.T) {
	store := newMemoryFilestore(t)
	server := httptest.NewServer(store)
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
	ctx := context.Background()

	emptyInfo := object.NewStaticObjectInfo("edge/empty.bin", time.Now(), 0, true, nil, f)
	empty, err := f.Put(ctx, strings.NewReader(""), emptyInfo)
	require.NoError(t, err)
	assert.Equal(t, int64(0), empty.Size())

	const specialPath = "edge/space # ü.txt"
	const contents = "unknown-size-data"
	unknownInfo := object.NewStaticObjectInfo(specialPath, time.Now(), -1, true, nil, f)
	unknown, err := f.Put(ctx, strings.NewReader(contents), unknownInfo)
	require.NoError(t, err)
	assert.Equal(t, int64(len(contents)), unknown.Size())
	reader, err := unknown.Open(ctx)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, contents, string(data))
}

func TestVFSFullCacheReadWriteAndReadonly(t *testing.T) {
	store := newMemoryFilestore(t)
	server := httptest.NewServer(store)
	defer server.Close()
	f := newTestFs(t, server.URL, "/outputs", testFilesystemID, "token")
	ctx := context.Background()

	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeFull
	opt.CacheDir = t.TempDir()
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0
	virtual := vfs.New(ctx, f, &opt)
	defer virtual.Shutdown()

	require.NoError(t, virtual.Mkdir("vfs", 0o755))
	handle, err := virtual.OpenFile("vfs/random.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = handle.WriteString("abcdefghij")
	require.NoError(t, err)
	_, err = handle.WriteAt([]byte("XYZ"), 3)
	require.NoError(t, err)
	require.NoError(t, handle.Truncate(8))
	require.NoError(t, handle.Sync())
	require.NoError(t, handle.Close())
	virtual.WaitForWriters(2 * time.Second)
	assert.Equal(t, vfs.DrainStatus{}, virtual.DrainStatus())

	remote, err := f.NewObject(ctx, "vfs/random.bin")
	require.NoError(t, err)
	reader, err := remote.Open(ctx)
	require.NoError(t, err)
	contents, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "abcXYZgh", string(contents))

	handle, err = virtual.OpenFile("vfs/random.bin", os.O_WRONLY|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = handle.WriteString("short")
	require.NoError(t, err)
	require.NoError(t, handle.Close())
	virtual.WaitForWriters(2 * time.Second)
	require.NoError(t, virtual.Rename("vfs/random.bin", "vfs/renamed.bin"))
	require.NoError(t, virtual.Remove("vfs/renamed.bin"))
	require.NoError(t, virtual.Remove("vfs"))

	readOnlyOpt := opt
	readOnlyOpt.ReadOnly = true
	readOnlyOpt.CacheDir = t.TempDir()
	readOnly := vfs.New(ctx, f, &readOnlyOpt)
	defer readOnly.Shutdown()
	assert.ErrorIs(t, readOnly.Mkdir("forbidden", 0o755), vfs.EROFS)
	store.mu.Lock()
	assert.False(t, store.dirs["/outputs/forbidden"])
	store.mu.Unlock()
}
