# 大数据分析项目文档

## 目录
1. [项目概述](#项目概述)
2. [环境配置](#环境配置)
3. [快速开始](#快速开始)
4. [模块说明](#模块说明)
5. [使用示例](#使用示例)
6. [常见问题](#常见问题)

---

## 项目概述

本项目是一个完整的大数据分析框架，专门用于分析和预测应用负载规律。

### 核心功能
- **数据预处理**: 数据清洗、特征工程、时间序列特征提取
- **模型训练**: 支持 XGBoost、随机森林、LSTM 等多种算法
- **可视化分析**: 趋势图、热力图、相关性矩阵等
- **预测评估**: 多维度模型评估指标

### 技术栈
- Python 3.8+
- pandas, numpy - 数据处理
- scikit-learn - 机器学习
- xgboost, lightgbm - 梯度提升算法
- matplotlib, seaborn - 数据可视化
- Jupyter - 交互式分析

---

## 环境配置

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. VSCode 插件推荐
- **Python** (ms-python.python)
- **Jupyter** (ms-toolsai.jupyter)
- **Pylance** (ms-python.vscode-pylance)

### 3. 可选配置
如需使用深度学习模型（LSTM），请安装：
```bash
pip install tensorflow
```

---

## 快速开始

### 步骤 1: 准备数据
将您的原始数据文件（CSV/Excel）放入 `data/raw/` 目录

### 步骤 2: 数据探索
打开 Jupyter Notebook：
```bash
jupyter notebook notebooks/01_data_exploration.ipynb
```

按照 Notebook 的步骤进行：
- 加载数据
- 探索性分析
- 数据预处理
- 特征工程

### 步骤 3: 模型训练
```bash
jupyter notebook notebooks/02_model_training.ipynb
```

完成：
- 模型训练
- 性能评估
- 结果可视化

---

## 模块说明

### preprocessing 模块

#### DataLoader
```python
from src.preprocessing.data_loader import DataLoader

loader = DataLoader(data_dir="./data")
df = loader.load_csv("your_data.csv")
```

#### DataPreprocessor
```python
from src.preprocessing.preprocessor import DataPreprocessor

preprocessor = DataPreprocessor()

# 数据清洗
df_clean = preprocessor.clean_data(df)

# 时间特征提取
df_processed = preprocessor.extract_time_features(df_clean, 'timestamp')

# 滞后特征
df_processed = preprocessor.create_lag_features(df_processed, 'cpu_usage')

# 滚动统计特征
df_processed = preprocessor.create_rolling_features(df_processed, 'cpu_usage')
```

### models 模块

#### LoadPredictionModel
```python
from src.models.prediction_model import LoadPredictionModel

model = LoadPredictionModel(model_type='xgboost')

# 准备数据
X_train, X_test, y_train, y_test = model.prepare_data(df, 'target_column')

# 训练模型
model.train_xgboost(X_train, y_train)
# 或
model.train_random_forest(X_train, y_train)
# 或
model.train_lstm(X_train, y_train)

# 评估
metrics = model.evaluate(X_test, y_test)

# 特征重要性
importance = model.get_feature_importance(feature_names)
```

### visualization 模块

#### LoadVisualizer
```python
from src.visualization.plotter import LoadVisualizer

viz = LoadVisualizer()

# 趋势图
viz.plot_load_trend(df, 'timestamp', 'cpu_usage')

# 热力图
viz.plot_heatmap_by_hour(df, 'timestamp', 'cpu_usage')

# 分布图
viz.plot_distribution(df, 'cpu_usage')

# 相关性矩阵
viz.plot_correlation_matrix(df)

# 预测对比
viz.plot_prediction_vs_actual(y_test, y_pred)

# 特征重要性
viz.plot_feature_importance(importance_df)
```

---

## 使用示例

### 完整工作流
```python
import pandas as pd
from src.preprocessing.data_loader import DataLoader
from src.preprocessing.preprocessor import DataPreprocessor
from src.models.prediction_model import LoadPredictionModel
from src.visualization.plotter import LoadVisualizer

# 1. 加载数据
loader = DataLoader("./data/raw")
df = loader.load_csv("load_data.csv")

# 2. 数据清洗
preprocessor = DataPreprocessor()
df_clean = preprocessor.clean_data(df)

# 3. 特征工程
df_clean = preprocessor.extract_time_features(df_clean, 'timestamp')
df_clean = preprocessor.create_lag_features(df_clean, 'cpu_usage')
df_clean = preprocessor.create_rolling_features(df_clean, 'cpu_usage')

# 4. 可视化探索
viz = LoadVisualizer()
viz.plot_load_trend(df_clean.head(168), 'timestamp', 'cpu_usage')
viz.plot_heatmap_by_hour(df_clean, 'timestamp', 'cpu_usage')

# 5. 模型训练
model = LoadPredictionModel()
X_train, X_test, y_train, y_test = model.prepare_data(df_clean, 'cpu_usage')
model.train_xgboost(X_train, y_train)

# 6. 模型评估
metrics = model.evaluate(X_test, y_test)
print(f"RMSE: {metrics['RMSE']:.4f}")
print(f"MAE: {metrics['MAE']:.4f}")
print(f"R²: {metrics['R2']:.4f}")

# 7. 特征分析
importance = model.get_feature_importance(X_train.columns.tolist())
viz.plot_feature_importance(importance)
```

---

## 常见问题

### Q1: 如何处理缺失值？
A: `DataPreprocessor.clean_data()` 会自动处理：
- 数值型数据用中位数填充
- 分类型数据用众数填充

### Q2: 可以自定义特征吗？
A: 可以！在 `preprocessor.py` 中添加自定义的特征提取方法。

### Q3: 如何选择最佳模型？
A: 比较不同模型的 RMSE、MAE 和 R² 指标，选择综合表现最好的。

### Q4: 如何保存训练好的模型？
```python
import joblib
joblib.dump(model.model, "models/best_model.pkl")
```

### Q5: 数据格式要求？
A: 至少需要包含：
- 时间戳列（timestamp）
- 目标变量列（如 cpu_usage）

建议的时间格式：`YYYY-MM-DD HH:MM:SS`

---

## 项目结构
```
d:\bigdata\
├── data/
│   ├── raw/              # 原始数据
│   └── processed/        # 处理后的数据
├── notebooks/
│   ├── 01_data_exploration.ipynb    # 数据探索
│   └── 02_model_training.ipynb      # 模型训练
├── src/
│   ├── preprocessing/
│   │   ├── __init__.py
│   │   ├── data_loader.py
│   │   └── preprocessor.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── prediction_model.py
│   └── visualization/
│       ├── __init__.py
│       └── plotter.py
├── configs/
│   └── config.yaml       # 配置文件
├── docs/                 # 文档
├── README.md
├── requirements.txt
└── .gitignore
```

---

## 更新日志

### v1.0.0 (2024-03)
- ✅ 初始版本发布
- ✅ 数据预处理模块
- ✅ 多模型训练支持
- ✅ 可视化分析工具
- ✅ 完整的 Notebook 示例

---

## 联系方式

如有问题，请提交 Issue 或联系开发团队。
