package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

// todo:如果之前死掉的worker又活过来了，那么可能会让现在worker新创建的文件删除
// 清理MapTask需要写入文件中的脏数据 文件名称：./mr-out-TaskId-*
func CleanFileByMapId(MapTaskId int, path string) error {
	// 创建正则表达式，去匹配到所有的./mr-out-TaskId-*
	pattern := fmt.Sprintf(`^mr-out-%d-\d+$`, MapTaskId)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	// 读取当前目录下的所有文件
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	// 遍历读取的所有文件，去找匹配的文件
	for _, file := range files {
		if file.IsDir() {
			continue
		} // 只判断文件，跳过目录
		if regex.MatchString(file.Name()) { // 找到符合的文件并将其删除
			file_path := filepath.Join(path, file.Name())
			err := os.Remove(file_path)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func CleanFileByReduceId(ReduceTaskId int, path string) error {
	// 创建正则表达式，去匹配到所有的./mr-out-TaskId-*
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, ReduceTaskId)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	// 读取当前目录下的所有文件
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	// 遍历读取的所有文件，去找匹配的文件
	for _, file := range files {
		if file.IsDir() {
			continue
		} // 只判断文件，跳过目录
		if regex.MatchString(file.Name()) { // 找到符合的文件并将其删除
			file_path := filepath.Join(path, file.Name())
			err := os.Remove(file_path)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ReadSpecificFile(key_id int, path string) (fileList []*os.File, err error) {
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, key_id)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	// 读取当前目录下的所有文件
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// 遍历读取的所有文件，去找匹配的文件
	for _, file := range files {
		if file.IsDir() {
			continue
		} // 只判断文件，跳过目录
		if regex.MatchString(file.Name()) { // 找到符合的文件并将其删除
			file_ph := filepath.Join(path, file.Name())
			f, err := os.Open(file_ph)
			if err != nil {
				//log.Fatal("Can't open file " + file_ph)
				for _, oFile := range fileList {
					oFile.Close()
				}
				return nil, err
			}
			fileList = append(fileList, f) // 将匹配到的文件指针返回
		}
	}
	return fileList, nil
}
