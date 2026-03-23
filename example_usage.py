#!/usr/bin/env python3
"""
阿里云OSS大数据处理使用示例
"""

from oss_data_processor import OSSDataProcessor
import pandas as pd
import numpy as np
from datetime import datetime

# 自定义数据处理函数
def block_trace_analyzer(data_chunk, filename, chunk_num):
    """
    区块链轨迹数据分析函数
    根据实际数据格式进行定制
    """
    
    # 基本统计信息
    print(f"=== 处理 {filename} - 块 {chunk_num} ===")
    print(f"数据行数: {len(data_chunk)}")
    print(f"列名: {list(data_chunk.columns)}")
    
    # 数据预览
    print("数据预览:")
    print(data_chunk.head(2))
    
    # 基本统计分析
    if len(data_chunk) > 0:
        print("数值列统计:")
        numeric_cols = data_chunk.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            print(data_chunk[numeric_cols].describe())
    
    print("-" * 80)

def advanced_analysis(data_chunk, filename, chunk_num):
    """
    高级分析函数 - 根据实际需求定制
    """
    
    # 这里可以添加具体的分析逻辑
    # 例如：交易模式分析、异常检测、时间序列分析等
    
    # 示例：时间序列分析（如果包含时间戳）
    timestamp_cols = [col for col in data_chunk.columns if 'time' in col.lower() or 'date' in col.lower()]
    if timestamp_cols:
        print(f"发现时间戳列: {timestamp_cols}")
        # 进行时间序列分析
    
    # 示例：交易金额分析
    amount_cols = [col for col in data_chunk.columns if 'amount' in col.lower() or 'value' in col.lower()]
    if amount_cols:
        print(f"发现金额列: {amount_cols}")
        for col in amount_cols:
            if col in data_chunk.columns:
                print(f"{col}统计:")
                print(f"  总和: {data_chunk[col].sum():.2f}")
                print(f"  均值: {data_chunk[col].mean():.2f}")
                print(f"  最大值: {data_chunk[col].max():.2f}")
                print(f"  最小值: {data_chunk[col].min():.2f}")

def main():
    """主函数 - 演示各种使用方法"""
    
    url = "http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz"
    
    print("阿里云OSS区块链轨迹数据分析")
    print("=" * 60)
    
    # 创建处理器
    processor = OSSDataProcessor(url)
    
    try:
        # 1. 获取文件信息
        print("1. 获取文件信息...")
        file_size = processor.get_file_info()
        
        if file_size:
            estimated_time = file_size / (10 * 1024 * 1024)  # 假设10MB/s下载速度
            print(f"预计处理时间: {estimated_time/60:.1f} 分钟")
        
        # 2. 选择处理策略
        print("\n2. 选择处理策略:")
        print("   a) 流式处理小样本进行数据探索")
        print("   b) 下载部分数据进行详细分析")
        print("   c) 完整流式处理（需要较长时间）")
        
        # 这里演示流式处理小样本
        print("\n3. 开始流式处理小样本数据...")
        
        # 设置较小的块大小进行快速测试
        processor.stream_process(block_trace_analyzer, chunk_size_mb=5)
        
        # 4. 下载部分数据进行详细分析
        print("\n4. 下载前500MB数据进行详细分析...")
        processor.download_partial(0, 0.5, "block_traces_sample_500mb.tar.gz")
        
        print("\n处理完成！")
        print("下一步建议:")
        print("1. 根据数据探索结果定制分析函数")
        print("2. 调整chunk_size_mb参数优化性能")
        print("3. 使用分布式计算框架处理完整数据")
        
    except Exception as e:
        print(f"处理过程中出现错误: {e}")
    finally:
        processor.cleanup()

# 命令行使用示例
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # 命令行模式
        if sys.argv[1] == "stream":
            # 流式处理模式
            url = "http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz"
            processor = OSSDataProcessor(url)
            processor.stream_process(block_trace_analyzer)
            processor.cleanup()
        
        elif sys.argv[1] == "partial":
            # 部分下载模式
            url = "http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz"
            processor = OSSDataProcessor(url)
            
            start_gb = float(sys.argv[2]) if len(sys.argv) > 2 else 0
            end_gb = float(sys.argv[3]) if len(sys.argv) > 3 else 1
            output_file = sys.argv[4] if len(sys.argv) > 4 else f"partial_{start_gb}gb_to_{end_gb}gb.tar.gz"
            
            processor.download_partial(start_gb, end_gb, output_file)
            processor.cleanup()
    else:
        # 交互式演示
        main()