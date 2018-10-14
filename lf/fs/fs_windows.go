// +build windows

package lf

/*
Copyright 2018 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import (
	"os"

	"github.com/gravitational/trace"
)

// WriteLock not supported on Windows.
func WriteLock(f *os.File) error {
	return trace.BadParameter("file locking not supported on Windows")
}

// TryWriteLock not supported on Windows.
func TryWriteLock(f *os.File) error {
	return trace.BadParameter("file locking not supported on Windows")
}

// ReadLock not supported on Windows.
func ReadLock(f *os.File) error {
	return trace.BadParameter("file locking not supported on Windows")
}

// Unlock not supported on Windows.
func Unlock(f *os.File) error {
	return trace.BadParameter("file locking not supported on Windows")
}
