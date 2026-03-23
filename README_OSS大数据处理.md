# 阿里云OSS大数据处理解决方案

## 问题描述

- 数据文件：756GB的阿里云区块链轨迹数据
- 存储限制：100GB可用硬盘空间
- 需求：部分下载并进行分析

## 解决方案概览

### 1. 流式处理（推荐）

**优势**：

- 不保存完整文件到本地
- 内存占用可控
- 支持实时分析

**适用场景**：

- 数据探索和初步分析
- 需要快速了解数据结构的场景

### 2. 部分下载

**优势**：

- 下载指定范围的数据
- 可以多次下载不同部分
- 适合详细分析

**适用场景**：

- 需要保存部分数据到本地
- 分布式处理前的数据准备

## 快速开始

### 安装依赖

```bash
pip install requests pandas tqdm
```

### 基本使用

#### 方法1：流式处理测试

```python
# 运行示例脚本
python example_usage.py
```

#### 方法2：命令行使用

```bash
# 流式处理模式
python oss_data_processor.py --url "http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz" --mode stream

# 部分下载模式（下载前1GB）
python oss_data_processor.py --url "http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz" --mode partial --start-gb 0 --end-gb 1 --output sample_1gb.tar.gz
```

## 详细配置

### 内存优化设置

```python
# 在oss_data_processor.py中调整这些参数
processor = OSSDataProcessor(url, max_memory_gb=2)  # 限制内存使用
processor.stream_process(callback, chunk_size_mb=50)  # 调整块大小
```

### 数据处理函数定制

```python
def custom_analyzer(data_chunk, filename, chunk_num):
    """自定义分析函数"""
    # 1. 数据清洗
    cleaned_data = data_chunk.dropna()

    # 2. 特征工程
    if 'timestamp' in cleaned_data.columns:
        cleaned_data['hour'] = pd.to_datetime(cleaned_data['timestamp']).dt.hour

    # 3. 统计分析
    print(f"处理 {len(cleaned_data)} 行数据")

    # 4. 保存中间结果（可选）
    if chunk_num % 10 == 0:
        cleaned_data.to_csv(f"chunk_{chunk_num}.csv", index=False)
```

## 性能优化建议

### 1. 网络优化

```python
# 使用会话保持连接
session = requests.Session()

# 设置超时和重试
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)
```

### 2. 内存管理

```python
# 及时清理内存
import gc

def memory_efficient_processor(data_chunk, filename, chunk_num):
    # 处理数据
    result = heavy_computation(data_chunk)

    # 及时清理
    del data_chunk
    gc.collect()

    return result
```

### 3. 并行处理

```python
from concurrent.futures import ThreadPoolExecutor
import threading

class ParallelProcessor:
    def __init__(self, max_workers=4):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.lock = threading.Lock()

    def process_chunk(self, chunk_data):
        # 线程安全的数据处理
        with self.lock:
            return process_data(chunk_data)
```

## 高级功能

### 1. 数据采样

```python
def stratified_sampling(data_chunk, sample_ratio=0.1):
    """分层抽样"""
    if 'category' in data_chunk.columns:
        return data_chunk.groupby('category').apply(
            lambda x: x.sample(frac=sample_ratio)
        ).reset_index(drop=True)
    else:
        return data_chunk.sample(frac=sample_ratio)
```

### 2. 实时监控

```python
import psutil

def monitor_resources():
    """监控系统资源"""
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')

    print(f"内存使用: {memory.percent}%")
    print(f"磁盘使用: {disk.percent}%")

    if memory.percent > 90 or disk.percent > 90:
        print("警告: 资源使用过高!")
        return False
    return True
```

## 故障排除

### 常见问题

1. **网络连接问题**

   ```python
   # 检查网络连接
   try:
       response = requests.get(url, timeout=10)
   except requests.exceptions.Timeout:
       print("网络连接超时，请检查网络设置")
   ```

2. **内存不足**
   - 减小 `chunk_size_mb` 参数
   - 使用 `gc.collect()` 手动清理内存
   - 关闭不必要的应用程序

3. **磁盘空间不足**
   - 使用流式处理而非下载
   - 定期清理临时文件
   - 考虑使用外部存储

### 性能监控脚本

```python
# performance_monitor.py
import time
import psutil

def monitor_performance():
    start_time = time.time()

    while True:
        current_time = time.time()
        elapsed = current_time - start_time

        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('.')

        print(f"运行时间: {elapsed:.1f}s | 内存: {memory.percent}% | 磁盘: {disk.percent}%")

        if memory.percent > 95 or disk.percent > 95:
            print("资源使用过高，建议停止处理")
            break

        time.sleep(5)
```

## 下一步建议

1. **数据探索阶段**：使用流式处理了解数据结构
2. **分析阶段**：下载部分数据进行详细分析
3. **生产环境**：考虑使用Spark等分布式计算框架
4. **长期方案**：搭建大数据处理集群或使用云服务

## 联系方式

如有问题，请参考代码注释或查阅相关文档。
