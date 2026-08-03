package bunqueue

import (
	"reflect"
	"testing"
)

func TestNameDataWireSeparation(t *testing.T) {
	connection, socket := scriptedConnection(t)
	queue := NewQueueWithConnection("named", connection)
	data := map[string]any{"name": "customer-visible", "to": "a@b.c"}

	job, err := queue.Add("send-email", data, nil)
	if err != nil {
		t.Fatal(err)
	}
	command := decodeWrittenCommand(t, socket)
	if command["name"] != "send-email" || !reflect.DeepEqual(command["data"], data) {
		t.Fatalf("PUSH name/data were not separate: %#v", command)
	}
	if job.Name() != "send-email" || !reflect.DeepEqual(job.Data(), data) {
		t.Fatalf("producer stub lost name/data: name=%q data=%#v", job.Name(), job.Data())
	}
}

func TestBulkAndJobReadersPreserveModernAndLegacyPayloads(t *testing.T) {
	connection, socket := scriptedConnection(t)
	queue := NewQueueWithConnection("named-bulk", connection)
	_, err := queue.AddBulk([]BulkEntry{
		{Name: "object-job", Data: map[string]any{"name": "user-name", "value": 1}},
		{Name: "scalar-job", Data: false},
	})
	if err != nil {
		t.Fatal(err)
	}
	command := decodeWrittenCommand(t, socket)
	jobs := asSlice(command["jobs"])
	if asMap(jobs[0])["name"] != "object-job" || asMap(jobs[1])["data"] != false {
		t.Fatalf("PUSHB name/data were not separate: %#v", jobs)
	}

	modern := newJob(map[string]any{
		"name": "modern-op", "data": map[string]any{"name": "user-name", "value": 1},
	}, connection, "")
	legacy := newJob(map[string]any{
		"data": map[string]any{"name": "legacy-op", "value": 2},
	}, connection, "")
	scalar := newJob(map[string]any{"name": "scalar-op", "data": 42}, connection, "")

	if modern.Name() != "modern-op" || !reflect.DeepEqual(modern.Data(), modern.Raw["data"]) {
		t.Fatalf("modern job changed user data: name=%q data=%#v", modern.Name(), modern.Data())
	}
	if legacy.Name() != "legacy-op" || !reflect.DeepEqual(legacy.Data(), map[string]any{"value": 2}) {
		t.Fatalf("legacy envelope was not decoded: name=%q data=%#v", legacy.Name(), legacy.Data())
	}
	if scalar.Name() != "scalar-op" || !reflect.DeepEqual(scalar.Data(), 42) {
		t.Fatalf("scalar payload was not preserved: name=%q data=%#v", scalar.Name(), scalar.Data())
	}
}
