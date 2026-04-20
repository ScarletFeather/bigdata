#!/usr/bin/env python3
"""
数据分析模块
负责对清洗后的数据进行分析和可视化
"""

import pandas as pd
import numpy as np
from src.visualization.plotter import LoadVisualizer


class DataAnalyzer:
    """数据分析师"""
    
    def __init__(self):
        self.visualizer = LoadVisualizer()
    
    def analyze_numeric_features(self, df):
        """
        分析数值型特征
        """
        print("=== 数值型特征分析 ===")
        print("=" * 60)
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) == 0:
            print("没有数值型特征")
            return
        
        print(f"发现 {len(numeric_cols)} 个数值型特征:")
        print(numeric_cols.tolist())
        
        # 基本统计信息
        print("\n基本统计信息:")
        print(df[numeric_cols].describe())
        
        # 相关性分析
        if len(numeric_cols) > 1:
            print("\n特征相关性:")
            correlation_matrix = df[numeric_cols].corr()
            print(correlation_matrix)
            
            # 可视化相关性矩阵
            self.visualizer.plot_correlation_matrix(df)
    
    def analyze_categorical_features(self, df):
        """
        分析分类型特征
        """
        print("\n=== 分类型特征分析 ===")
        print("=" * 60)
        
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) == 0:
            print("没有分类型特征")
            return
        
        print(f"发现 {len(categorical_cols)} 个分类型特征:")
        print(categorical_cols.tolist())
        
        # 分析每个分类特征的分布
        for col in categorical_cols:
            print(f"\n{col} 分布:")
            print(df[col].value_counts())
    
    def analyze_time_series(self, df, time_column):
        """
        分析时间序列数据
        """
        if time_column not in df.columns:
            print(f"时间列 '{time_column}' 不存在")
            return
        
        print(f"\n=== 时间序列分析 ===")
        print("=" * 60)
        
        # 时间范围
        start_time = df[time_column].min()
        end_time = df[time_column].max()
        print(f"时间范围: {start_time} 到 {end_time}")
        print(f"时间跨度: {(end_time - start_time).days} 天")
        
        # 时间特征分析
        if hasattr(df[time_column], 'dt'):
            print("\n时间特征统计:")
            print(f"按小时分布:")
            print(df[time_column].dt.hour.value_counts().sort_index())
            print(f"按星期分布:")
            print(df[time_column].dt.dayofweek.value_counts().sort_index())
    
    def detect_anomalies(self, df, column):
        """
        异常值检测
        """
        if column not in df.columns:
            print(f"列 '{column}' 不存在")
            return
        
        print(f"\n=== {column} 异常值检测 ===")
        print("=" * 60)
        
        # 使用IQR方法检测异常值
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
        
        print(f"正常范围: [{lower_bound:.2f}, {upper_bound:.2f}]")
        print(f"异常值数量: {len(outliers)}")
        print(f"异常值比例: {len(outliers) / len(df) * 100:.2f}%")
        
        if len(outliers) > 0:
            print("异常值示例:")
            print(outliers[[column]].head())
    
    def comprehensive_analysis(self, df, time_column=None):
        """
        综合分析
        """
        print("=== 综合数据分析 ===")
        print("=" * 60)
        
        # 数据基本信息
        print(f"数据形状: {df.shape}")
        print(f"列名: {list(df.columns)}")
        print(f"缺失值统计:")
        print(df.isnull().sum())
        
        # 分析数值型特征
        self.analyze_numeric_features(df)
        
        # 分析分类型特征
        self.analyze_categorical_features(df)
        
        # 分析时间序列（如果有时间列）
        if time_column:
            self.analyze_time_series(df, time_column)
        
        # 异常值检测（对所有数值列）
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols[:3]:  # 只检测前3个数值列，避免输出过多
            self.detect_anomalies(df, col)


def main():
    """
    主函数
    """
    # 示例用法
    import pandas as pd
    import numpy as np
    
    # 创建示例数据
    date_range = pd.date_range('2020-01-01', periods=100, freq='H')
    data = {
        'timestamp': date_range,
        'cpu_usage': np.random.normal(50, 10, 100),
        'memory_usage': np.random.normal(60, 15, 100),
        'network_traffic': np.random.normal(100, 20, 100),
        'category': np.random.choice(['A', 'B', 'C'], 100)
    }
    
    df = pd.DataFrame(data)
    
    # 创建分析师实例
    analyzer = DataAnalyzer()
    
    # 综合分析
    analyzer.comprehensive_analysis(df, 'timestamp')


if __name__ == "__main__":
    main()
