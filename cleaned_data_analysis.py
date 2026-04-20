#!/usr/bin/env python3
"""
清洗后数据的分析和可视化脚本
"""

import pandas as pd
import numpy as np
from src.preprocessing.preprocessor import DataPreprocessor
from src.visualization.plotter import LoadVisualizer
from src.preprocessing.oss_data_processor import OSSDataProcessor


def analyze_cleaned_data():
    """
    分析和可视化清洗后的数据
    """
    print("=== 清洗后数据的分析和可视化 ===")
    print("=" * 60)
    
    # 创建可视化工具
    visualizer = LoadVisualizer()
    preprocessor = DataPreprocessor()
    
    # 1. 使用模拟数据进行演示
    print("\n1. 使用模拟数据进行分析和可视化演示")
    print("-" * 40)
    
    # 创建包含时间序列的模拟数据
    date_range = pd.date_range('2020-01-01', periods=100, freq='H')
    data = {
        'timestamp': date_range,
        'cpu_usage': np.random.normal(50, 10, 100),
        'memory_usage': np.random.normal(60, 15, 100),
        'network_traffic': np.random.normal(100, 20, 100),
        'category': np.random.choice(['A', 'B', 'C'], 100)
    }
    
    # 添加一些缺失值
    data['cpu_usage'][::10] = np.nan
    data['memory_usage'][::15] = np.nan
    
    df = pd.DataFrame(data)
    
    # 清洗数据
    cleaned_df = preprocessor.clean_data(df)
    
    # 提取时间特征
    cleaned_df = preprocessor.extract_time_features(cleaned_df, 'timestamp')
    
    # 可视化分析
    print("\n绘制CPU使用率趋势图...")
    visualizer.plot_load_trend(cleaned_df, 'timestamp', 'cpu_usage', title='CPU使用率趋势')
    
    print("\n绘制CPU使用率分布...")
    visualizer.plot_distribution(cleaned_df, 'cpu_usage', title='CPU使用率分布')
    
    print("\n绘制特征相关性矩阵...")
    visualizer.plot_correlation_matrix(cleaned_df, title='特征相关性矩阵')
    
    print("\n绘制小时-星期热力图...")
    visualizer.plot_heatmap_by_hour(cleaned_df, 'timestamp', 'cpu_usage')


def process_and_analyze_real_data():
    """
    处理和分析真实数据
    """
    print("\n=== 处理和分析真实数据 ===")
    print("=" * 60)
    
    # 创建处理器
    url = "http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz"
    processor = OSSDataProcessor(url)
    visualizer = LoadVisualizer()
    preprocessor = DataPreprocessor()
    
    # 存储处理后的数据
    processed_data = []
    
    def analysis_processor(data_chunk, filename, chunk_num):
        """
        带分析功能的处理函数
        """
        print(f"\n处理 {filename} - 块 {chunk_num}")
        
        # 清洗数据
        cleaned_chunk = preprocessor.clean_data(data_chunk)
        
        # 提取时间特征（如果有时间列）
        time_cols = [col for col in cleaned_chunk.columns if 'time' in col.lower() or 'date' in col.lower()]
        if time_cols:
            try:
                cleaned_chunk = preprocessor.extract_time_features(cleaned_chunk, time_cols[0])
            except Exception as e:
                print(f"提取时间特征失败: {e}")
        
        # 存储处理后的数据
        processed_data.append(cleaned_chunk)
        
        # 基本可视化分析
        if len(cleaned_chunk) > 0:
            # 选择数值列进行分析
            numeric_cols = cleaned_chunk.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                # 绘制第一个数值列的分布
                print(f"\n绘制 {numeric_cols[0]} 分布...")
                visualizer.plot_distribution(cleaned_chunk, numeric_cols[0])
                
                # 绘制相关性矩阵
                if len(numeric_cols) > 1:
                    print("\n绘制特征相关性矩阵...")
                    visualizer.plot_correlation_matrix(cleaned_chunk)
        
        return cleaned_chunk
    
    # 流式处理并分析数据
    print("开始流式处理并分析数据...")
    processor.stream_process(analysis_processor, chunk_size_mb=5)
    
    # 合并所有处理后的数据
    if processed_data:
        combined_df = pd.concat(processed_data, ignore_index=True)
        print(f"\n合并后数据形状: {combined_df.shape}")
        
        # 保存清洗后的数据
        output_file = "cleaned_block_traces.csv"
        combined_df.to_csv(output_file, index=False)
        print(f"\n清洗后的数据已保存到: {output_file}")
    
    processor.cleanup()


def main():
    """
    主函数
    """
    # 运行模拟数据分析
    analyze_cleaned_data()
    
    # 运行真实数据分析
    process_and_analyze_real_data()
    
    print("\n=== 分析和可视化完成 ===")
    print("清洗后的数据可以用于后续的预测模型训练")


if __name__ == "__main__":
    main()
