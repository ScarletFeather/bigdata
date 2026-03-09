"""
数据预处理模块
负责数据清洗、转换和特征工程
"""
import pandas as pd
import numpy as np
from datetime import datetime


class DataPreprocessor:
    """数据预处理器"""
    
    def __init__(self):
        pass
    
    def clean_data(self, df):
        """
        数据清洗
        - 处理缺失值
        - 去除重复值
        - 处理异常值
        """
        # 删除完全重复的行
        df = df.drop_duplicates()
        
        # 处理缺失值（可根据实际情况调整策略）
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().sum() > 0:
                # 数值型数据用中位数填充
                df[col] = df[col].fillna(df[col].median())
        
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if df[col].isnull().sum() > 0:
                # 分类型数据用众数填充
                df[col] = df[col].fillna(df[col].mode()[0] if len(df[col].mode()) > 0 else 'Unknown')
        
        return df
    
    def extract_time_features(self, df, time_column='timestamp'):
        """
        提取时间特征
        适用于负载数据的时间序列分析
        """
        if time_column not in df.columns:
            raise ValueError(f"列 '{time_column}' 不存在")
        
        df[time_column] = pd.to_datetime(df[time_column])
        
        # 提取时间特征
        df['hour'] = df[time_column].dt.hour
        df['day_of_week'] = df[time_column].dt.dayofweek
        df['day_of_month'] = df[time_column].dt.day
        df['month'] = df[time_column].dt.month
        df['year'] = df[time_column].dt.year
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_peak_hour'] = ((df['hour'] >= 9) & (df['hour'] <= 18)).astype(int)
        
        return df
    
    def create_lag_features(self, df, target_column, lag_periods=[1, 2, 3, 7, 14]):
        """
        创建滞后特征
        用于时间序列预测
        """
        for lag in lag_periods:
            df[f'{target_column}_lag_{lag}'] = df[target_column].shift(lag)
        
        return df
    
    def create_rolling_features(self, df, target_column, windows=[3, 7, 14]):
        """
        创建滚动统计特征
        """
        for window in windows:
            df[f'{target_column}_rolling_mean_{window}'] = df[target_column].rolling(window=window).mean()
            df[f'{target_column}_rolling_std_{window}'] = df[target_column].rolling(window=window).std()
        
        return df
