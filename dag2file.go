package merkledag

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Hash2File 从 KVStore 中读取哈希值对应的数据，并根据路径返回对应的文件内容
func Hash2File(store KVStore, hash []byte, path string) ([]byte, error) {
	// 从 KVStore 中获取哈希值对应的数据
	data, err := store.Get(hash)
	if err != nil {
		return nil, fmt.Errorf("error retrieving data from KVStore: %v", err)
	}

	// 解码数据
	var obj Object
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("error decoding data: %v", err)
	}

	// 根据路径找到对应的文件内容
	content, err := getContent(&obj, path, store)
	if err != nil {
		return nil, err
	}

	return content, nil
}

// getContent 根据路径获取文件内容
func getContent(obj *Object, path string, store KVStore) ([]byte, error) {
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid path")
	}

	currentObj := obj
	for _, part := range parts {
		if part == "" {
			continue
		}

		var found bool
		for _, link := range currentObj.Links {
			if link.Name == part {
				found = true

				// 根据链接类型获取内容
				if link.Name == "data" {
					return currentObj.Data, nil
				} else {
					// 从 KVStore 中递归获取子对象的数据
					data, err := store.Get(link.Hash)
					if err != nil {
						return nil, fmt.Errorf("error retrieving data from KVStore: %v", err)
					}

					// 解码子对象
					var childObj Object
					if err := json.Unmarshal(data, &childObj); err != nil {
						return nil, fmt.Errorf("error decoding data: %v", err)
					}

					// 更新当前对象为子对象，继续遍历路径
					currentObj = &childObj
					break
				}
			}
		}

		if !found {
			return nil, fmt.Errorf("file or directory not found: %s", part)
		}
	}

	return nil, fmt.Errorf("invalid path")
}
