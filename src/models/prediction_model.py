"""
负载预测模型模块
包含多种预测算法
"""
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')


class LoadPredictionModel:
    """应用负载预测模型基类"""
    
    def __init__(self, model_type='xgboost'):
        self.model_type = model_type
        self.model = None
        self.metrics = {}
    
    def prepare_data(self, df, target_column, test_size=0.2):
        """
        准备训练和测试数据
        """
        # 删除含有 NaN 的行
        df_clean = df.dropna()
        
        # 分离特征和目标变量
        feature_cols = [col for col in df_clean.columns if col != target_column]
        X = df_clean[feature_cols]
        y = df_clean[target_column]
        
        # 划分训练集和测试集
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, shuffle=False
        )
        
        return X_train, X_test, y_train, y_test
    
    def train_xgboost(self, X_train, y_train):
        """训练 XGBoost 模型"""
        try:
            import xgboost as xgb
            self.model = xgb.XGBRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=5,
                random_state=42
            )
            self.model.fit(X_train, y_train)
            print("XGBoost 模型训练完成")
        except ImportError:
            print("请先安装 xgboost: pip install xgboost")
            raise
    
    def train_random_forest(self, X_train, y_train):
        """训练随机森林模型"""
        from sklearn.ensemble import RandomForestRegressor
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        self.model.fit(X_train, y_train)
        print("随机森林模型训练完成")
    
    def train_lstm(self, X_train, y_train, sequence_length=60):
        """
        训练 LSTM 模型（深度学习）
        需要额外的数据预处理步骤
        """
        try:
            from tensorflow.keras.models import Sequential
            from tensorflow.keras.layers import LSTM, Dense, Dropout
            
            # 数据重塑为 LSTM 输入格式 [samples, time steps, features]
            X_train_reshaped = X_train.values.reshape((X_train.shape[0], 1, X_train.shape[1]))
            
            self.model = Sequential([
                LSTM(50, activation='relu', input_shape=(1, X_train.shape[1])),
                Dropout(0.2),
                Dense(1)
            ])
            
            self.model.compile(optimizer='adam', loss='mse')
            self.model.fit(X_train_reshaped, y_train, epochs=20, batch_size=32, verbose=1)
            print("LSTM 模型训练完成")
        except ImportError:
            print("请先安装 tensorflow: pip install tensorflow")
            raise
    
    def evaluate(self, X_test, y_test):
        """
        评估模型性能
        """
        if self.model is None:
            raise ValueError("模型尚未训练")
        
        # 预测
        if hasattr(self.model, 'predict'):
            y_pred = self.model.predict(X_test)
        else:
            # LSTM 需要特殊处理
            X_test_reshaped = X_test.values.reshape((X_test.shape[0], 1, X_test.shape[1]))
            y_pred = self.model.predict(X_test_reshaped)
        
        # 计算评估指标
        self.metrics = {
            'RMSE': np.sqrt(mean_squared_error(y_test, y_pred)),
            'MAE': mean_absolute_error(y_test, y_pred),
            'R2': r2_score(y_test, y_pred)
        }
        
        print("\n模型评估结果:")
        print(f"RMSE (均方根误差): {self.metrics['RMSE']:.4f}")
        print(f"MAE (平均绝对误差): {self.metrics['MAE']:.4f}")
        print(f"R² (决定系数): {self.metrics['R2']:.4f}")
        
        return self.metrics
    
    def get_feature_importance(self, feature_names, top_n=10):
        """
        获取特征重要性（仅适用于树模型）
        """
        if not hasattr(self.model, 'feature_importances_'):
            print("该模型不支持特征重要性分析")
            return None
        
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"\nTop {top_n} 重要特征:")
        print(importance_df.head(top_n))
        
        return importance_df
