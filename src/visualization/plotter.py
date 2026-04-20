"""
可视化模块
用于数据探索和结果展示
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime


class LoadVisualizer:
    """应用负载可视化工具"""
    
    def __init__(self, style='default'):
        plt.style.use(style)
        self.figsize = (14, 7)
    
    def plot_load_trend(self, df, time_column, load_column, title='应用负载趋势图', show=False):
        """
        绘制负载趋势图
        """
        plt.figure(figsize=self.figsize)
        plt.plot(df[time_column], df[load_column], linewidth=1)
        plt.title(title, fontsize=16)
        plt.xlabel('时间', fontsize=12)
        plt.ylabel('负载值', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        if show:
            plt.show()
    
    def plot_heatmap_by_hour(self, df, time_column, load_column, show=False):
        """
        绘制小时 - 星期热力图
        展示不同时间段和星期的负载分布
        """
        df_copy = df.copy()
        df_copy[time_column] = pd.to_datetime(df_copy[time_column])
        df_copy['hour'] = df_copy[time_column].dt.hour
        df_copy['day_of_week'] = df_copy[time_column].dt.dayofweek
        
        # 创建透视表
        pivot_table = df_copy.pivot_table(
            values=load_column,
            index='hour',
            columns='day_of_week',
            aggfunc='mean'
        )
        
        # 绘制热力图
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_table, annot=True, fmt='.1f', cmap='YlOrRd', 
                    cbar_kws={'label': '平均负载'})
        plt.title('应用负载热力图（按小时和星期）', fontsize=16)
        plt.xlabel('星期', fontsize=12)
        plt.ylabel('小时', fontsize=12)
        plt.xticks(ticks=range(7), labels=['一', '二', '三', '四', '五', '六', '日'])
        plt.tight_layout()
        if show:
            plt.show()
    
    def plot_distribution(self, df, column, title='数据分布', show=False):
        """
        绘制数据分布直方图
        """
        fig, axes = plt.subplots(1, 2, figsize=self.figsize)
        
        # 直方图
        axes[0].hist(df[column], bins=30, edgecolor='black', alpha=0.7)
        axes[0].set_title(f'{column} - 直方图', fontsize=14)
        axes[0].set_xlabel(column)
        axes[0].set_ylabel('频数')
        axes[0].grid(True, alpha=0.3)
        
        # 箱线图
        axes[1].boxplot(df[column], vert=True)
        axes[1].set_title(f'{column} - 箱线图', fontsize=14)
        axes[1].set_ylabel(column)
        axes[1].grid(True, alpha=0.3)
        
        plt.suptitle(title, fontsize=16)
        plt.tight_layout()
        if show:
            plt.show()
    
    def plot_correlation_matrix(self, df, title='特征相关性矩阵', show=False):
        """
        绘制特征相关性矩阵
        """
        # 只选择数值型列
        numeric_df = df.select_dtypes(include=[np.number])
        
        if numeric_df.shape[1] == 0:
            print("没有数值型特征")
            return
        
        plt.figure(figsize=(12, 10))
        correlation_matrix = numeric_df.corr()
        
        # 使用掩码隐藏上三角
        mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
        
        sns.heatmap(correlation_matrix, mask=mask, annot=True, fmt='.2f', 
                    cmap='coolwarm', center=0, square=True, linewidths=.5,
                    cbar_kws={"shrink": .5})
        
        plt.title(title, fontsize=16)
        plt.tight_layout()
        if show:
            plt.show()
    
    def plot_prediction_vs_actual(self, y_actual, y_pred, title='预测值 vs 实际值', show=False):
        """
        绘制预测值与实际值对比图
        """
        fig, axes = plt.subplots(1, 2, figsize=self.figsize)
        
        # 散点图
        axes[0].scatter(y_actual, y_pred, alpha=0.5)
        axes[0].plot([y_actual.min(), y_actual.max()], 
                     [y_actual.min(), y_actual.max()], 
                     'r--', lw=2)
        axes[0].set_xlabel('实际值', fontsize=12)
        axes[0].set_ylabel('预测值', fontsize=12)
        axes[0].set_title('预测值 vs 实际值', fontsize=14)
        axes[0].grid(True, alpha=0.3)
        
        # 残差图
        residuals = y_actual - y_pred
        axes[1].scatter(y_pred, residuals, alpha=0.5)
        axes[1].axhline(y=0, color='r', linestyle='--')
        axes[1].set_xlabel('预测值', fontsize=12)
        axes[1].set_ylabel('残差', fontsize=12)
        axes[1].set_title('残差图', fontsize=14)
        axes[1].grid(True, alpha=0.3)
        
        plt.suptitle(title, fontsize=16)
        plt.tight_layout()
        if show:
            plt.show()
    
    def plot_feature_importance(self, importance_df, top_n=15, title='特征重要性', show=False):
        """
        绘制特征重要性图
        """
        plt.figure(figsize=(10, max(6, top_n * 0.4)))
        
        top_features = importance_df.head(top_n)
        
        sns.barplot(data=top_features, x='importance', y='feature', 
                   palette='viridis')
        
        plt.title(title, fontsize=16)
        plt.xlabel('重要性', fontsize=12)
        plt.ylabel('特征', fontsize=12)
        plt.tight_layout()
        if show:
            plt.show()
