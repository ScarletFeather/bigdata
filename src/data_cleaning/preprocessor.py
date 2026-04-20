"""
数据预处理模块
负责数据清洗、转换和特征工程
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataPreprocessor:
    """数据预处理器"""
    
    def __init__(self, config=None):
        """
        初始化数据预处理器
        Args:
            config: 配置字典，包含清洗参数
        """
        self.config = config or {}
        self.missing_value_strategy = self.config.get('missing_value_strategy', 'median')
        self.outlier_strategy = self.config.get('outlier_strategy', 'iqr')
        logger.info("数据预处理器初始化完成")
    
    def clean_data(self, df):
        """
        数据清洗
        - 处理缺失值
        - 去除重复值
        - 处理异常值
        - 数据类型转换
        """
        logger.info(f"开始数据清洗，原始数据形状: {df.shape}")
        
        # 1. 删除完全重复的行
        initial_rows = len(df)
        df = df.drop_duplicates()
        duplicate_count = initial_rows - len(df)
        if duplicate_count > 0:
            logger.info(f"删除了 {duplicate_count} 条重复记录")
        
        # 2. 处理缺失值
        df = self._handle_missing_values(df)
        
        # 3. 处理异常值
        df = self._handle_outliers(df)
        
        # 4. 数据类型转换
        df = self._convert_data_types(df)
        
        logger.info(f"数据清洗完成，清洗后数据形状: {df.shape}")
        return df
    
    def _handle_missing_values(self, df):
        """
        处理缺失值
        """
        # 统计缺失值
        missing_stats = df.isnull().sum()
        total_missing = missing_stats.sum()
        
        if total_missing > 0:
            logger.info(f"发现 {total_missing} 个缺失值")
            logger.info(f"缺失值分布: {dict(missing_stats[missing_stats > 0])}")
        else:
            logger.info("无缺失值")
            return df
        
        # 处理数值型列
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().sum() > 0:
                if self.missing_value_strategy == 'mean':
                    fill_value = df[col].mean()
                elif self.missing_value_strategy == 'median':
                    fill_value = df[col].median()
                elif self.missing_value_strategy == 'zero':
                    fill_value = 0
                else:
                    fill_value = df[col].median()
                
                df[col] = df[col].fillna(fill_value)
                logger.info(f"填充 {col} 的缺失值，使用 {self.missing_value_strategy}: {fill_value}")
        
        # 处理分类型列
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if df[col].isnull().sum() > 0:
                # 分类型数据用众数填充
                if len(df[col].mode()) > 0:
                    fill_value = df[col].mode()[0]
                else:
                    fill_value = 'Unknown'
                
                df[col] = df[col].fillna(fill_value)
                logger.info(f"填充 {col} 的缺失值，使用众数: {fill_value}")
        
        return df
    
    def _handle_outliers(self, df):
        """
        处理异常值
        """
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if self.outlier_strategy == 'iqr':
                # 使用IQR方法检测异常值
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # 替换异常值为边界值
                df[col] = np.clip(df[col], lower_bound, upper_bound)
                logger.info(f"处理 {col} 的异常值，使用 IQR 方法，边界: [{lower_bound:.2f}, {upper_bound:.2f}]")
            elif self.outlier_strategy == 'zscore':
                # 使用Z-score方法检测异常值
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                threshold = 3
                
                # 替换异常值为均值
                df.loc[z_scores > threshold, col] = df[col].mean()
                outlier_count = (z_scores > threshold).sum()
                if outlier_count > 0:
                    logger.info(f"处理 {col} 的 {outlier_count} 个异常值，使用 Z-score 方法")
        
        return df
    
    def _convert_data_types(self, df):
        """
        数据类型转换
        """
        # 尝试将对象类型转换为数值类型
        for col in df.columns:
            if df[col].dtype == 'object':
                # 尝试转换为数值类型
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                    if df[col].dtype != 'object':
                        logger.info(f"将 {col} 转换为数值类型: {df[col].dtype}")
                except Exception as e:
                    logger.warning(f"转换 {col} 为数值类型失败: {e}")
            
            # 尝试将时间字符串转换为时间类型
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col], errors='ignore')
                    if df[col].dtype != 'object':
                        logger.info(f"将 {col} 转换为时间类型")
                except Exception as e:
                    logger.warning(f"转换 {col} 为时间类型失败: {e}")
        
        return df
    
    def extract_time_features(self, df, time_column='timestamp'):
        """
        提取时间特征
        适用于负载数据的时间序列分析
        """
        if time_column not in df.columns:
            logger.error(f"列 '{time_column}' 不存在")
            raise ValueError(f"列 '{time_column}' 不存在")
        
        try:
            df[time_column] = pd.to_datetime(df[time_column])
        except Exception as e:
            logger.error(f"转换 {time_column} 为时间类型失败: {e}")
            raise
        
        # 提取时间特征
        df['hour'] = df[time_column].dt.hour
        df['day_of_week'] = df[time_column].dt.dayofweek
        df['day_of_month'] = df[time_column].dt.day
        df['month'] = df[time_column].dt.month
        df['year'] = df[time_column].dt.year
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_peak_hour'] = ((df['hour'] >= 9) & (df['hour'] <= 18)).astype(int)
        
        # 提取更详细的时间特征
        df['quarter'] = df[time_column].dt.quarter
        df['day_of_year'] = df[time_column].dt.dayofyear
        df['week_of_year'] = df[time_column].dt.isocalendar().week
        
        logger.info(f"提取时间特征完成，新增 {7} 个时间特征")
        return df
    
    def create_lag_features(self, df, target_column, lag_periods=[1, 2, 3, 7, 14]):
        """
        创建滞后特征
        用于时间序列预测
        """
        if target_column not in df.columns:
            logger.error(f"目标列 '{target_column}' 不存在")
            raise ValueError(f"目标列 '{target_column}' 不存在")
        
        for lag in lag_periods:
            df[f'{target_column}_lag_{lag}'] = df[target_column].shift(lag)
            logger.info(f"创建滞后特征: {target_column}_lag_{lag}")
        
        # 填充滞后特征的缺失值
        for lag in lag_periods:
            lag_col = f'{target_column}_lag_{lag}'
            if df[lag_col].isnull().sum() > 0:
                df[lag_col] = df[lag_col].fillna(df[target_column].median())
                logger.info(f"填充 {lag_col} 的缺失值")
        
        return df
    
    def create_rolling_features(self, df, target_column, windows=[3, 7, 14]):
        """
        创建滚动统计特征
        """
        if target_column not in df.columns:
            logger.error(f"目标列 '{target_column}' 不存在")
            raise ValueError(f"目标列 '{target_column}' 不存在")
        
        for window in windows:
            df[f'{target_column}_rolling_mean_{window}'] = df[target_column].rolling(window=window).mean()
            df[f'{target_column}_rolling_std_{window}'] = df[target_column].rolling(window=window).std()
            df[f'{target_column}_rolling_min_{window}'] = df[target_column].rolling(window=window).min()
            df[f'{target_column}_rolling_max_{window}'] = df[target_column].rolling(window=window).max()
            logger.info(f"创建滚动特征，窗口大小: {window}")
        
        # 填充滚动特征的缺失值
        for window in windows:
            for stat in ['mean', 'std', 'min', 'max']:
                col_name = f'{target_column}_rolling_{stat}_{window}'
                if df[col_name].isnull().sum() > 0:
                    df[col_name] = df[col_name].fillna(df[target_column].median())
                    logger.info(f"填充 {col_name} 的缺失值")
        
        return df
    
    def drop_unnecessary_columns(self, df, columns_to_drop=None):
        """
        删除不必要的列
        """
        if columns_to_drop:
            # 只删除存在的列
            existing_columns = [col for col in columns_to_drop if col in df.columns]
            if existing_columns:
                df = df.drop(columns=existing_columns)
                logger.info(f"删除不必要的列: {existing_columns}")
            else:
                logger.warning("没有要删除的列")
        return df
    
    def scale_features(self, df, columns=None, method='minmax'):
        """
        特征缩放
        """
        if not columns:
            columns = df.select_dtypes(include=[np.number]).columns
        
        for col in columns:
            if method == 'minmax':
                min_val = df[col].min()
                max_val = df[col].max()
                if max_val > min_val:
                    df[f'{col}_scaled'] = (df[col] - min_val) / (max_val - min_val)
                    logger.info(f"使用 Min-Max 缩放 {col}")
            elif method == 'standard':
                mean_val = df[col].mean()
                std_val = df[col].std()
                if std_val > 0:
                    df[f'{col}_scaled'] = (df[col] - mean_val) / std_val
                    logger.info(f"使用 Standard 缩放 {col}")
        
        return df
    
    def run_pipeline(self, df, time_column=None, target_column=None, columns_to_drop=None):
        """
        运行完整的数据预处理 pipeline
        """
        logger.info("开始数据预处理 pipeline")
        
        # 1. 清洗数据
        df = self.clean_data(df)
        
        # 2. 删除不必要的列
        if columns_to_drop:
            df = self.drop_unnecessary_columns(df, columns_to_drop)
        
        # 3. 提取时间特征
        if time_column:
            df = self.extract_time_features(df, time_column)
        
        # 4. 创建滞后特征
        if target_column:
            df = self.create_lag_features(df, target_column)
            df = self.create_rolling_features(df, target_column)
        
        # 5. 特征缩放
        df = self.scale_features(df)
        
        logger.info("数据预处理 pipeline 完成")
        return df
