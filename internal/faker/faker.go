package faker

import (
	"errors"
	"math/rand"
	"time"

	fake "syreclabs.com/go/faker"
)

var (
	errRemoteCall = errors.New("RPC failed")
)

// fakeData generates some dummy data for script input/outputs.
func fakeData() map[string]string {
	data := make(map[string]string)
	for i := 0; i < rand.Intn(3); i++ {
		data[fake.RandomChoice([]string{"user", "group", "domain", "account"})] = fake.Internet().DomainWord()
	}
	return data
}

// JobID generates a fake job ID.
func JobID() string {
	return fake.Code().Abn()
}

// JobName generates a fake job name.
func JobName() string {
	return fake.RandomChoice([]string{"delete_user", "create_user", "lock_user", "unlock_user"})
}

// JobInput generates some fake job input.
func JobInput() map[string]string {
	return fakeData()
}

// JobOutput generates some fake job output.
func JobOutput() map[string]string {
	data := fakeData()
	data["status"] = "success"
	return data
}

// RemoteCall simulates interaction with a remote system.
//
// It adds a delay of 0-2 seconds.
//
// ~1/10 of responses will return an error.
func RemoteCall() (err error) {
	time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
	if rand.Intn(10) == 0 {
		return errRemoteCall
	}
	return
}
