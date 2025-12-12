package kvdb

type Marshallable interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) error
}

// KeyValueStore is a generic interface for a key-value store.
// K is the type of the keys, which must be comparable. If K is not a common int, string, or []byte type, the implementation
// must implement BinaryMarshaler and BinaryUnmarshaler for the key type. The resulting binary representation must preserve the comparison order.
// T is the type of the values, it has to implement encoding.BinaryMarshaler and encoding.BinaryUnmarshaler.
type KeyValueStore[K comparable, T any] interface {
	Put(key K, value T) error
	Get(key K) (T, error)
	Has(key K) (bool, error)
	Delete(key K) error
	Range(from, to K, fn func(key K, value T) error) error
	Collect(from, to K) ([]T, error)
}

type BinaryKeyValueStore interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
	Delete(key []byte) error
	Range(from, to []byte, fn func(key []byte, value []byte) error) error
}
