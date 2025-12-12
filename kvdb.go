package kvdb

import (
	"encoding/binary"

	"go.kvsh.ch/stderr"
)

// Make sure we implement the simple KeyValueStore interface.
var _ KeyValueStore[any, Marshallable] = (*KeyValueStoreImpl[any, Marshallable, BinaryKeyValueStore])(nil)

type KeyValueStoreImpl[K comparable, T any, B BinaryKeyValueStore] struct {
	Store B
}

func NewKeyValueStore[K comparable, T Marshallable, B BinaryKeyValueStore](store B) *KeyValueStoreImpl[K, T, B] {
	return &KeyValueStoreImpl[K, T, B]{
		Store: store,
	}
}

func (kvs *KeyValueStoreImpl[K, T, B]) marshallKey(key K) ([]byte, error) {
	// The implementation of this function depends on the type of K. The binary representation
	// should be consistent and unique for each key and maintains the same comparison order as K.
	switch k := any(key).(type) {
	// Common types with straightforward binary representations
	case string:
		return []byte(k), nil
	// Byte slices and common fixed-size byte arrays
	case []byte:
		return k, nil
	case [16]byte:
		return k[:], nil
	case [32]byte:
		return k[:], nil
	case [64]byte:
		return k[:], nil
	// Integer types are converted to big-endian byte slices
	case int8:
		return []byte{byte(k)}, nil
	case uint8:
		return []byte{byte(k)}, nil
	case int16:
		bs := make([]byte, 2)
		binary.BigEndian.PutUint16(bs, uint16(k))
		return bs, nil
	case uint16:
		bs := make([]byte, 2)
		binary.BigEndian.PutUint16(bs, k)
		return bs, nil
	case int32:
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, uint32(k))
		return bs, nil
	case uint32:
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, k)
		return bs, nil
	case int64:
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(k))
		return bs, nil
	case uint64:
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, k)
		return bs, nil
	case int:
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(k))
		return bs, nil
	case uint:
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(k))
		return bs, nil
	default:
		if m, ok := k.(Marshallable); ok {
			return m.Marshal()
		}

	}
	return nil, stderr.ErrUnimplemented
}

func (kvs *KeyValueStoreImpl[K, T, B]) unmarshallKey(data []byte) (K, error) {
	var key K
	switch any(key).(type) {
	// Common types with straightforward binary representations
	case string:
		return any(string(data)).(K), nil
	// Byte slices and common fixed-size byte arrays
	case []byte:
		return any(data).(K), nil
	case [16]byte:
		var arr [16]byte
		copy(arr[:], data)
		return any(arr).(K), nil
	case [32]byte:
		var arr [32]byte
		copy(arr[:], data)
		return any(arr).(K), nil
	case [64]byte:
		var arr [64]byte
		copy(arr[:], data)
		return any(arr).(K), nil
	// Integer types are converted from big-endian byte slices. Slice size is 4 or 8 bytes.
	case int8:
		if len(data) != 1 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for int8 key")
		}
		val := int8(data[0])
		return any(val).(K), nil
	case uint8:
		if len(data) != 1 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for uint8 key")
		}
		val := data[0]
		return any(val).(K), nil
	case int16:
		if len(data) != 2 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for int16 key")
		}
		val := int16(binary.BigEndian.Uint16(data))
		return any(val).(K), nil
	case uint16:
		if len(data) != 2 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for uint16 key")
		}
		val := binary.BigEndian.Uint16(data)
		return any(val).(K), nil
	case int32:
		if len(data) != 4 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for int32 key")
		}
		val := int32(binary.BigEndian.Uint32(data))
		return any(val).(K), nil
	case uint32:
		if len(data) != 4 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for uint32 key")
		}
		val := binary.BigEndian.Uint32(data)
		return any(val).(K), nil
	case int64:
		if len(data) != 8 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for int64 key")
		}
		val := int64(binary.BigEndian.Uint64(data))
		return any(val).(K), nil
	case uint64:
		if len(data) != 8 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for uint64 key")
		}
		val := binary.BigEndian.Uint64(data)
		return any(val).(K), nil
	case int:
		if len(data) != 8 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for int key")
		}
		val := int(binary.BigEndian.Uint64(data))
		return any(val).(K), nil
	case uint:
		if len(data) != 8 {
			return key, stderr.Wrap(stderr.ErrIncorrectSize, "invalid data length for uint key")
		}
		val := uint(binary.BigEndian.Uint64(data))
		return any(val).(K), nil
	default:
		if u, ok := any(key).(Marshallable); ok {
			err := u.Unmarshal(data)
			if err != nil {
				return key, err
			}
			return any(u).(K), nil
		}
	}
	return key, stderr.ErrUnimplemented
}

func (kvs *KeyValueStoreImpl[K, T, B]) Put(key K, value T) error {
	k, err := kvs.marshallKey(key)
	if err != nil {
		return err
	}
	v, err := (any)(value).(Marshallable).Marshal()
	if err != nil {
		return err
	}
	return kvs.Store.Put(k, v)
}

func (kvs *KeyValueStoreImpl[K, T, B]) Get(key K) (T, error) {
	k, err := kvs.marshallKey(key)
	if err != nil {
		var value T
		return value, err
	}
	v, err := kvs.Store.Get(k)
	if err != nil {
		var value T
		return value, err
	}
	var value T
	err = (any)(value).(Marshallable).Unmarshal(v)
	return value, err
}

func (kvs *KeyValueStoreImpl[K, T, B]) Has(key K) (bool, error) {
	k, err := kvs.marshallKey(key)
	if err != nil {
		return false, err
	}
	return kvs.Store.Has(k)
}

func (kvs *KeyValueStoreImpl[K, T, B]) Delete(key K) error {
	k, err := kvs.marshallKey(key)
	if err != nil {
		return err
	}
	return kvs.Store.Delete(k)
}

func (kvs *KeyValueStoreImpl[K, T, B]) Range(from, to K, fn func(key K, value T) error) error {
	f, err := kvs.marshallKey(from)
	if err != nil {
		return err
	}
	t, err := kvs.marshallKey(to)
	if err != nil {
		return err
	}
	return kvs.Store.Range(f, t, func(kb []byte, vb []byte) error {
		k, err := kvs.unmarshallKey(kb)
		if err != nil {
			return err
		}
		var v T
		err = (any)(v).(Marshallable).Unmarshal(vb)
		if err != nil {
			return err
		}
		return fn(k, v)
	})
}

func (kvs *KeyValueStoreImpl[K, T, B]) Collect(from, to K) ([]T, error) {
	r := []T{}
	err := kvs.Range(from, to, func(key K, value T) error {
		r = append(r, value)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}
