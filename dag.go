package merkledag

import (
	"encoding/json"
	"hash"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	if node.Type() == FILE {
		file := node.(File)
		fileSlice := sliceFile(file, store, h)
		jsonData, _ := json.Marshal(fileSlice)
		hash := calculateHash(jsonData, h)
		return hash
	} else {
		dir := node.(Dir)
		dirSlice := sliceDir(dir, store, h)
		jsonData, _ := json.Marshal(dirSlice)
		hash := calculateHash(jsonData, h)
		return hash
	}
}

func dfsForSlice(hight int, node File, store KVStore, seedId int, h hash.Hash) (*Object, int) {
	if hight == 1 {
		data := node.Bytes()[seedId:]
		blob := Object{Data: data, Links: nil}
		jsonData, _ := json.Marshal(blob)
		hash := calculateHash(jsonData, h)
		store.Put(hash, data)
		return &blob, len(data)
	}
	links := &Object{}
	totalLen := 0
	for i := 1; i <= 4096; i++ {
		if seedId >= len(node.Bytes()) {
			break
		}
		child, childLen := dfsForSlice(hight-1, node, store, seedId, h)
		totalLen += childLen
		jsonData, _ := json.Marshal(child)
		hash := calculateHash(jsonData, h)
		store.Put(hash, jsonData)
		links.Links = append(links.Links, Link{
			Hash: hash,
			Size: childLen,
		})
		typeName := "link"
		if child.Links == nil {
			typeName = "data"
		}
		links.Data = append(links.Data, []byte(typeName)...)
	}
	jsonData, _ := json.Marshal(links)
	hash := calculateHash(jsonData, h)
	store.Put(hash, jsonData)
	return links, totalLen
}

func sliceFile(node File, store KVStore, h hash.Hash) *Object {
	if len(node.Bytes()) <= 256*1024 {
		data := node.Bytes()
		blob := Object{Data: data, Links: nil}
		jsonData, _ := json.Marshal(blob)
		hash := calculateHash(jsonData, h)
		store.Put(hash, data)
		return &blob
	}
	linkLen := (len(node.Bytes()) + (256*1024 - 1)) / (256 * 1024)
	hight := 0
	tmp := linkLen
	for {
		hight++
		tmp /= 4096
		if tmp == 0 {
			break
		}
	}
	res, _ := dfsForSlice(hight, node, store, 0, h)
	return res
}

func sliceDir(node Dir, store KVStore, h hash.Hash) *Object {
	tree := &Object{}
	iter := node.It()
	for iter.Next() {
		elem := iter.Node()
		var elemType string
		var size int
		var name string
		var childObj *Object
		switch elem.Type() {
		case FILE:
			file := elem.(File)
			childObj = sliceFile(file, store, h)
			elemType = "link"
			size = int(file.Size())
			name = file.Name()
		case DIR:
			dir := elem.(Dir)
			childObj = sliceDir(dir, store, h)
			elemType = "tree"
			size = int(dir.Size())
			name = dir.Name()
		}
		jsonData, _ := json.Marshal(childObj)
		hash := calculateHash(jsonData, h)
		store.Put(hash, jsonData)
		tree.Links = append(tree.Links, Link{
			Hash: hash,
			Size: size,
			Name: name,
		})
		tree.Data = append(tree.Data, []byte(elemType)...)
	}
	jsonData, _ := json.Marshal(tree)
	hash := calculateHash(jsonData, h)
	store.Put(hash, jsonData)
	return tree
}

func calculateHash(data []byte, h hash.Hash) []byte {
	h.Reset()
	h.Write(data)
	return h.Sum(nil)
}
