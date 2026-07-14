package bunqueue

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// testServer spawns `bun src/main.ts` on a random port (real server e2e).
type testServer struct {
	port     int
	httpPort int
	dataDir  string
	cmd      *exec.Cmd
	extraEnv map[string]string
}

var shared *testServer

func TestMain(m *testing.M) {
	shared = newTestServer(nil)
	if err := shared.start(); err != nil {
		fmt.Fprintln(os.Stderr, "server start failed:", err)
		os.Exit(1)
	}
	code := m.Run()
	shared.stop()
	os.Exit(code)
}

func newTestServer(extraEnv map[string]string) *testServer {
	dir, _ := os.MkdirTemp("", "bunqueue-go-e2e-")
	return &testServer{
		port:     freePort(),
		httpPort: freePort(),
		dataDir:  dir,
		extraEnv: extraEnv,
	}
}

func (s *testServer) start() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	cmd := exec.Command("bun", "src/main.ts")
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"TCP_PORT="+fmt.Sprint(s.port),
		"HTTP_PORT="+fmt.Sprint(s.httpPort),
		"BUNQUEUE_DATA_PATH="+filepath.Join(s.dataDir, "bunq.db"),
	)
	for key, value := range s.extraEnv {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return err
	}
	s.cmd = cmd
	deadline := time.Now().Add(15 * time.Second)
	address := fmt.Sprintf("127.0.0.1:%d", s.port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.stop()
	return fmt.Errorf("bunqueue server did not start within 15s")
}

// crash kills the process hard (crash simulation) keeping port + data dir.
func (s *testServer) crash() {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_, _ = s.cmd.Process.Wait()
		s.cmd = nil
	}
}

func (s *testServer) stop() {
	s.crash()
	_ = os.RemoveAll(s.dataDir)
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "src", "main.ts")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("src/main.ts not found above %s", dir)
		}
		dir = parent
	}
}

func freePort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func uniqueName(prefix string) string {
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return prefix + "-" + hex.EncodeToString(buf)
}

func waitUntil(t *testing.T, timeout time.Duration, predicate func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func testQueue(t *testing.T, prefix string) *Queue {
	t.Helper()
	queue := NewQueue(uniqueName(prefix), Options{Port: shared.port})
	t.Cleanup(func() {
		_ = queue.Obliterate()
		queue.Close()
	})
	return queue
}

// startWorker runs the worker loop in a goroutine and stops it on cleanup.
func startWorker(t *testing.T, worker *Worker) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		worker.Run()
		close(done)
	}()
	t.Cleanup(func() {
		worker.Stop()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Log("worker did not stop within 15s")
		}
	})
}
