"""
将 HTTP chunk 迭代器包装成 file-like read() 接口。
供 StreamingTarProcessor 使用，用于 gzip → tar 流式管道。
"""


class IterReader:
    """将 generator-based 迭代器包装成 file-like read(size) 接口"""

    def __init__(self, response_iter):
        self._iter = response_iter
        self.buf = b''
        self.done = False
        self.bytes_total = 0

    def read(self, size=-1):
        if self.done and not self.buf:
            return b''
        while size < 0 or len(self.buf) < size:
            try:
                chunk = next(self._iter)
                if not chunk:
                    self.done = True
                    break
                self.buf += chunk
                self.bytes_total += len(chunk)
            except StopIteration:
                self.done = True
                break
        if size < 0:
            data, self.buf = self.buf, b''
        elif self.buf:
            data, self.buf = self.buf[:size], self.buf[size:]
        else:
            data = b''
        return data
