#!/usr/bin/env python3
"""
数据清洗示例脚本
展示如何使用 DataPreprocessor 进行数据清洗
"""

import pandas as pd
import numpy as np
from src.preprocessing.preprocessor import DataPreprocessor
from src.preprocessing.oss_data_processor import OSSDataProcessor


def data_cleaning_demo():
    """
    数据清洗演示函数
    """
    print("=== 数据清洗示例 ===")
    print("=" * 60)
    
    # 创建数据预处理器
    preprocessor = DataPreprocessor()
    
    # 1. 示例1：使用模拟数据进行清洗
    print("\n1. 使用模拟数据进行清洗演示")
    print("-" * 40)
    
    # 创建包含缺失值、重复值的模拟数据
    data = {
        'timestamp': pd.date_range('2020-01-01', periods=10),
        'value': [10, 20, np.nan, 40, 50, 20, 70, 80, np.nan, 100],
        'category': ['A', 'B', np.nan, 'A', 'B', 'B', 'A', np.nan, 'B', 'A']
    }
    
    df = pd.DataFrame(data)
    print("原始数据:")
    print(df)
    print(f"\n缺失值统计:")
    print(df.isnull().sum())
    print(f"重复值数量: {df.duplicated().sum()}")
    
    # 清洗数据
    cleaned_df = preprocessor.clean_data(df)
    print("\n清洗后的数据:")
    print(cleaned_df)
    print(f"\n清洗后缺失值统计:")
    print(cleaned_df.isnull().sum())
    print(f"清洗后重复值数量: {cleaned_df.duplicated().sum()}")
    
    # 2. 示例2：提取时间特征
    print("\n2. 提取时间特征演示")
    print("-" * 40)
    
    df_with_time_features = preprocessor.extract_time_features(cleaned_df, 'timestamp')
    print("提取时间特征后的数据:")
    print(df_with_time_features)
    
    # 3. 示例3：创建滞后和滚动特征
    print("\n3. 创建滞后和滚动特征演示")
    print("-" * 40)
    
    df_with_lag = preprocessor.create_lag_features(df_with_time_features, 'value')
    df_with_features = preprocessor.create_rolling_features(df_with_lag, 'value')
    print("创建特征后的数据:")
    print(df_with_features)


def process_real_data():
    """
    处理真实数据的示例
    """
    print("\n=== 处理真实数据示例 ===")
    print("=" * 60)
    
    # 使用之前下载的测试数据
    test_file = "test_data_1gb.tar.gz"
    
    print(f"使用测试文件: {test_file}")
    
    # 创建处理器
    processor = OSSDataProcessor("http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz")
    preprocessor = DataPreprocessor()
    
    # 定义带清洗功能的处理函数
    def cleaning_processor(data_chunk, filename, chunk_num):
        print(f"\n处理 {filename} - 块 {chunk_num}")
        print(f"原始数据形状: {data_chunk.shape}")
        
        # 清洗数据
        cleaned_chunk = preprocessor.clean_data(data_chunk)
        print(f"清洗后数据形状: {cleaned_chunk.shape}")
        
        # 提取时间特征（如果有时间列）
        time_cols = [col for col in cleaned_chunk.columns if 'time' in col.lower() or 'date' in col.lower()]
        if time_cols:
            print(f"发现时间列: {time_cols}")
            try:
                cleaned_chunk = preprocessor.extract_time_features(cleaned_chunk, time_cols[0])
                print(f"提取时间特征后数据形状: {cleaned_chunk.shape}")
            except Exception as e:
                print(f"提取时间特征失败: {e}")
        
        # 基本统计信息
        print("清洗后基本统计:")
        print(cleaned_chunk.describe())
        
        return cleaned_chunk
    
    # 流式处理并清洗数据
    print("开始流式处理并清洗数据...")
    processor.stream_process(cleaning_processor, chunk_size_mb=5)
    processor.cleanup()


if __name__ == "__main__":
    # 运行示例
    data_cleaning_demo()
    
    # 处理真实数据
    process_real_data()
    
    print("\n=== 数据清洗示例完成 ===")
    print("下一步：将清洗后的数据用于分析和预测")
