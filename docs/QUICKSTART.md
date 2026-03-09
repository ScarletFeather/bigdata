# 快速开始指南

## 🚀 5 分钟快速上手

### 步骤 1: 安装 Python 依赖包

打开终端（PowerShell 或 CMD），在项目目录下运行：

```bash
pip install -r requirements.txt
```

**或者双击运行** `start.bat` **文件**，它会自动检查并安装依赖。

### 步骤 2: 生成示例数据（可选）

如果您还没有真实数据，可以先生成示例数据用于测试：

```bash
python src/generate_sample_data.py
```

这会在 `data/raw/` 目录下生成 90 天的模拟负载数据。

### 步骤 3: 启动 Jupyter Notebook

```bash
jupyter notebook notebooks
```

浏览器会自动打开，然后：

1. 先打开 `01_data_exploration.ipynb` - 进行数据探索分析
2. 再打开 `02_model_training.ipynb` - 训练预测模型

---

## 📦 依赖包说明

### 核心依赖（必需）
- pandas - 数据处理
- numpy - 数值计算
- scikit-learn - 机器学习
- matplotlib - 绘图
- seaborn - 统计可视化
- jupyter - 交互式环境

### 可选依赖
- **xgboost** - 梯度提升算法（推荐安装，性能好）
- **tensorflow** - 深度学习（如需使用 LSTM 模型）
- **prophet** - Facebook 时间序列预测库
- **sqlalchemy** - 数据库连接

---

## 📁 项目结构说明

```
d:\bigdata\
│
├── data/                    # 数据目录
│   ├── raw/                # 放置原始数据文件
│   └── processed/          # 存放处理后的数据
│
├── notebooks/              # Jupyter Notebook 文件
│   ├── 01_data_exploration.ipynb    # ① 数据探索（从这里开始）
│   └── 02_model_training.ipynb      # ② 模型训练
│
├── src/                    # Python 源代码
│   ├── preprocessing/      # 数据预处理模块
│   │   ├── data_loader.py         # 数据加载
│   │   └── preprocessor.py        # 数据清洗和特征工程
│   ├── models/           # 机器学习模型
│   │   └── prediction_model.py    # 预测模型（XGBoost、随机森林等）
│   └── visualization/    # 可视化工具
│       └── plotter.py            # 各种图表绘制函数
│
├── configs/              # 配置文件
│   └── config.yaml      # 项目配置
│
├── docs/                # 文档目录
│   └── guide.md        # 详细使用文档
│
├── README.md           # 项目说明
├── requirements.txt    # Python 依赖列表
└── start.bat          # Windows 快速启动脚本
```

---

## 💻 VSCode 插件推荐

安装以下插件获得最佳开发体验：

### 必装插件
1. **Python** (ms-python.python)
2. **Jupyter** (ms-toolsai.jupyter)
3. **Pylance** (ms-python.vscode-pylance)

### 选装插件
- GitHub Copilot - AI 代码辅助
- CodeGeeX - 国产 AI 编程助手
- SQL Tools - 数据库工具

---

## 🔍 使用流程

### 场景 1: 我有数据文件，想分析负载规律

1. 将 CSV/Excel 文件放入 `data/raw/` 目录
2. 修改 `notebooks/01_data_exploration.ipynb` 中的文件名
3. 运行 Notebook，查看分析结果
4. 继续运行 `02_model_training.ipynb` 训练模型

### 场景 2: 我想先试试功能

1. 运行 `python src/generate_sample_data.py` 生成示例数据
2. 打开 `notebooks/01_data_exploration.ipynb`
3. 按顺序执行所有单元格

### 场景 3: 我想用数据库的数据

1. 在 `configs/config.yaml` 中配置数据库连接信息
2. 使用 `DataLoader.load_from_db()` 方法加载数据
3. 后续步骤同上

---

## ⚙️ 自定义配置

编辑 `configs/config.yaml` 文件：

```yaml
# 修改数据路径
DATA_PATH:
  raw: "./data/raw"
  processed: "./data/processed"

# 配置数据库（如需要）
DATABASE:
  type: "mysql"
  host: "localhost"
  port: 3306
  database: "your_database"
  user: "your_username"
  password: "your_password"

# 选择模型类型
MODEL:
  type: "lstm"  # 可选：xgboost, random_forest, lstm, prophet
```

---

## 🐛 常见问题

### Q: 安装依赖时出错怎么办？
A: 尝试升级 pip：
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Q: Jupyter 无法启动？
A: 检查是否正确安装了 jupyter：
```bash
pip install --upgrade jupyter notebook
```

### Q: 中文显示乱码？
A: Notebook 中已设置中文字体，如果仍有问题，请安装：
- Windows: 系统自带中文字体
- Mac: `pip install matplotlib-font-manager`

### Q: 如何更换自己的数据？
A: 
1. 确保数据包含时间戳列和数值列
2. 时间格式建议为 `YYYY-MM-DD HH:MM:SS`
3. 修改 Notebook 中的文件名即可

---

## 📞 获取帮助

- 查看详细文档：`docs/guide.md`
- 查看代码示例：Notebook 文件中有详细注释
- 遇到问题可搜索相关库的官方文档

---

## ✨ 下一步

完成基础分析后，您可以：

1. 尝试不同的模型（LSTM、Prophet 等）
2. 添加自定义特征
3. 调整模型参数优化性能
4. 将模型部署到生产环境
5. 设置定时任务自动更新预测

**现在就开始您的大数据分析之旅吧！** 🎉
