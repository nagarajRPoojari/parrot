package errors

import "fmt"

const KeyNotFoundError = IO("Key not found")
const FileNotFoundError = IO("File not found")

type IO string

func (t IO) Error() string {
	return fmt.Sprintf("io err: %s", string(t))
}
