#!/usr/bin/env python3
"""
阿里云OSS大数据文件处理工具
支持有限硬盘空间下处理756GB大数据文件
"""

import requests
import tarfile
import io
import pandas as pd
import os
from tqdm import tqdm
import argparse
import tempfile
from pathlib import Path

class OSSDataProcessor:
    def __init__(self, url, max_memory_gb=2):
        self.url = url
        self.max_memory_gb = max_memory_gb
        self.temp_dir = tempfile.mkdtemp(prefix="oss_data_")
        
    def get_file_info(self):
        """获取文件信息"""
        try:
            response = requests.head(self.url)
            content_length = response.headers.get('content-length')
            if content_length:
                size_gb = int(content_length) / (1024**3)
                print(f"文件大小: {size_gb:.2f} GB")
                return int(content_length)
            else:
                print("无法获取文件大小信息")
                return None
        except Exception as e:
            print(f"获取文件信息失败: {e}")
            return None
    
    def stream_process(self, process_callback, chunk_size_mb=100):
        """
        流式处理数据
        Args:
            process_callback: 数据处理回调函数
            chunk_size_mb: 每次处理的数据块大小(MB)
        """
        chunk_size = chunk_size_mb * 1024 * 1024  # 转换为字节
        
        print("开始流式处理数据...")
        
        try:
            response = requests.get(self.url, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            
            if total_size == 0:
                print("警告: 无法获取文件大小，使用默认处理方式")
                # 直接处理整个流
                self._process_stream(response, process_callback)
            else:
                # 分块处理
                self._process_by_chunks(response, total_size, chunk_size, process_callback)
                
        except Exception as e:
            print(f"流式处理失败: {e}")
    
    def _process_stream(self, response, process_callback):
        """处理整个数据流"""
        tar_stream = io.BytesIO()
        
        for chunk in tqdm(response.iter_content(chunk_size=8192), 
                         unit='B', unit_scale=True, desc="下载进度"):
            if chunk:
                tar_stream.write(chunk)
        
        tar_stream.seek(0)
        self._extract_and_process(tar_stream, process_callback)
    
    def _process_by_chunks(self, response, total_size, chunk_size, process_callback):
        """分块处理数据"""
        downloaded = 0
        
        with tqdm(total=total_size, unit='B', unit_scale=True, desc="下载进度") as pbar:
            while downloaded < total_size:
                end_byte = min(downloaded + chunk_size - 1, total_size - 1)
                
                headers = {'Range': f'bytes={downloaded}-{end_byte}'}
                chunk_response = requests.get(self.url, headers=headers, stream=True)
                
                # 处理当前数据块
                self._process_chunk(chunk_response, process_callback)
                
                downloaded += chunk_size
                pbar.update(chunk_size)
    
    def _process_chunk(self, response, process_callback):
        """处理单个数据块"""
        tar_stream = io.BytesIO()
        
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                tar_stream.write(chunk)
        
        tar_stream.seek(0)
        self._extract_and_process(tar_stream, process_callback)
    
    def _extract_and_process(self, tar_stream, process_callback):
        """解压并处理tar文件"""
        try:
            with tarfile.open(fileobj=tar_stream, mode='r:gz') as tar:
                for member in tar:
                    if member.isfile():
                        print(f"处理文件: {member.name}")
                        
                        # 根据文件类型选择处理方式
                        if member.name.endswith(('.csv', '.txt')):
                            self._process_csv_file(tar, member, process_callback)
                        elif member.name.endswith('.json'):
                            self._process_json_file(tar, member, process_callback)
                        else:
                            print(f"跳过不支持的文件类型: {member.name}")
        except tarfile.ReadError as e:
            print(f"解压错误 (可能是部分文件): {e}")
    
    def _process_csv_file(self, tar, member, process_callback):
        """处理CSV文件"""
        file_obj = tar.extractfile(member)
        
        # 逐块读取CSV文件
        for chunk_num, chunk in enumerate(pd.read_csv(file_obj, chunksize=10000)):
            print(f"处理CSV块 {chunk_num + 1}, 行数: {len(chunk)}")
            
            # 调用用户定义的处理函数
            if process_callback:
                process_callback(chunk, member.name, chunk_num)
    
    def _process_json_file(self, tar, member, process_callback):
        """处理JSON文件"""
        file_obj = tar.extractfile(member)
        
        # 逐行读取JSON文件
        for line_num, line in enumerate(file_obj):
            if line_num % 1000 == 0:
                print(f"处理JSON行 {line_num}")
            
            # 这里可以添加JSON处理逻辑
            if process_callback:
                process_callback(line.decode('utf-8'), member.name, line_num)
    
    def download_partial(self, start_gb, end_gb, output_file):
        """
        下载部分数据
        Args:
            start_gb: 起始位置(GB)
            end_gb: 结束位置(GB)
            output_file: 输出文件路径
        """
        start_byte = int(start_gb * 1024 * 1024 * 1024)
        end_byte = int(end_gb * 1024 * 1024 * 1024)
        
        print(f"下载数据范围: {start_gb}-{end_gb} GB")
        
        headers = {'Range': f'bytes={start_byte}-{end_byte}'}
        response = requests.get(self.url, headers=headers, stream=True)
        
        total_size = end_byte - start_byte + 1
        
        with open(output_file, 'wb') as f, \
             tqdm(total=total_size, unit='B', unit_scale=True, desc="下载进度") as pbar:
            
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
        
        print(f"部分数据已保存到: {output_file}")
    
    def cleanup(self):
        """清理临时文件"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            print(f"已清理临时目录: {self.temp_dir}")

# 示例处理函数
def sample_data_processor(data_chunk, filename, chunk_num):
    """示例数据处理函数"""
    print(f"处理 {filename} 的第 {chunk_num} 个数据块")
    print(f"数据形状: {data_chunk.shape if hasattr(data_chunk, 'shape') else 'N/A'}")
    print(f"前几行数据:")
    print(data_chunk.head(3) if hasattr(data_chunk, 'head') else data_chunk[:3])
    print("-" * 50)

def main():
    parser = argparse.ArgumentParser(description='阿里云OSS大数据处理工具')
    parser.add_argument('--url', required=True, help='OSS文件URL')
    parser.add_argument('--mode', choices=['stream', 'partial'], default='stream', 
                       help='处理模式: stream(流式处理) 或 partial(部分下载)')
    parser.add_argument('--start-gb', type=float, default=0, help='部分下载起始位置(GB)')
    parser.add_argument('--end-gb', type=float, default=1, help='部分下载结束位置(GB)')
    parser.add_argument('--output', help='输出文件路径(仅partial模式使用)')
    
    args = parser.parse_args()
    
    processor = OSSDataProcessor(args.url)
    
    try:
        # 获取文件信息
        file_size = processor.get_file_info()
        
        if args.mode == 'stream':
            # 流式处理模式
            processor.stream_process(sample_data_processor, chunk_size_mb=50)
        else:
            # 部分下载模式
            if not args.output:
                args.output = f"partial_data_{args.start_gb}gb_to_{args.end_gb}gb.tar.gz"
            
            processor.download_partial(args.start_gb, args.end_gb, args.output)
    
    except KeyboardInterrupt:
        print("\n用户中断处理")
    except Exception as e:
        print(f"处理过程中出现错误: {e}")
    finally:
        processor.cleanup()

if __name__ == "__main__":
    # 示例用法
    url = "http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz"
    
    # 创建处理器实例
    processor = OSSDataProcessor(url)
    
    # 获取文件信息
    file_size = processor.get_file_info()
    
    # 方法1: 流式处理前1GB数据进行测试
    print("\n=== 方法1: 流式处理测试 ===")
    processor.stream_process(sample_data_processor, chunk_size_mb=10)
    
    # 方法2: 下载前1GB数据到本地
    print("\n=== 方法2: 部分下载测试 ===")
    processor.download_partial(0, 1, "test_data_1gb.tar.gz")
    
    processor.cleanup()