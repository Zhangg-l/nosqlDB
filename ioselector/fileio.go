package ioselector

import "os"

type FileIOSelector struct {
	fd *os.File
}

func NewFileIOSelector(fName string, fsize int64) (IOSelector, error) {
	var (
		file *os.File
		err  error
	)

	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}

	if file, err = openFile(fName, fsize); err != nil {
		return nil, err
	}
	return &FileIOSelector{
		fd: file}, nil
}

func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {

	// 从偏移位置写len(b)个长度
	return fio.fd.WriteAt(b, offset)
}

func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	// 从偏移位置读len(b)
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIOSelector) Close() error {
	return  fio.fd.Close()
}
func (fio *FileIOSelector) Delete() error {
	 if err := fio.fd.Close();err != nil{
		return err
	 }
	 return os.Remove(fio.fd.Name())
}
