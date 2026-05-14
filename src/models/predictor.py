"""
时间序列预测模块 v2
支持多种回归/时间序列模型，用于 I/O 负载预测和准确性评估

核心改进:
  1. 多窗口聚合：自动尝试 0.2s / 0.5s / 1s 窗口，选择最优粒度
  2. 递归多步预测：每步预测后再用于下一步输入，而非一次性预测
  3. 丰富特征工程：趋势、动量、EWM、变化率等
  4. 新增模型：指数平滑、ARIMA、自回归
  5. 模型自动选择：根据验证集指标选出最优模型
  6. 自适应配置：根据数据量自动调整 n_lags 和 predict_steps
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    mean_squared_error, mean_absolute_error, r2_score,
    mean_absolute_percentage_error
)

logger = logging.getLogger(__name__)

plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


# ================================================================
# 辅助类
# ================================================================

class ExponentialSmoothingForecaster:
    """简单指数平滑预测器（适合小数据量）"""

    def __init__(self, alpha=0.3):
        self.alpha = alpha
        self.level = None

    def fit(self, y_train):
        # 用所有历史均值初始化
        self.level = np.mean(y_train)
        return self

    def predict(self, steps):
        # 指数平滑扩展预测：预测值 = 最后一个平滑值
        return np.full(steps, self.level)


class AR1Forecaster:
    """一阶自回归预测器（适合有趋势的时间序列）"""

    def __init__(self):
        self.coef = 0.0
        self.intercept = 0.0
        self.last_values = []

    def fit(self, y_train):
        # AR(1): y_t = a * y_{t-1} + b + noise
        y = np.array(y_train)
        X = np.column_stack([np.ones(len(y) - 1), y[:-1]])
        y_target = y[1:]
        coeffs = np.linalg.lstsq(X, y_target, rcond=None)[0]
        self.intercept = coeffs[0]
        self.coef = coeffs[1]
        self.last_values = list(y[-3:])  # 保留最近几个值用于递推
        return self

    def predict(self, steps):
        preds = []
        current = self.last_values[:]
        for _ in range(steps):
            next_val = self.intercept + self.coef * current[-1]
            preds.append(next_val)
            current.append(next_val)
        return np.array(preds)


class TrendForecaster:
    """趋势外推预测器（适合有明显线性趋势的数据）"""

    def __init__(self, poly_degree=2):
        self.poly_degree = poly_degree
        self.model = None

    def fit(self, y_train):
        x = np.arange(len(y_train))
        self.model = np.polyfit(x, np.array(y_train), self.poly_degree)
        return self

    def predict(self, steps):
        x_future = np.arange(len(self.model), len(self.model) + steps)
        return np.polyval(self.model, x_future)


class MovingAverageForecaster:
    """移动平均预测器（适合波动较小、变化缓慢的数据）"""

    def __init__(self, window=3):
        self.window = window

    def fit(self, y_train):
        self.last_values = list(np.array(y_train)[-self.window:])
        return self

    def predict(self, steps):
        preds = []
        current = self.last_values[:]
        for _ in range(steps):
            pred = np.mean(current[-self.window:])
            preds.append(pred)
            current.append(pred)
        return np.array(preds)


# ================================================================
# 主预测器
# ================================================================

class TimeSeriesPredictor:
    """
    I/O 负载时间序列预测器 v2
    """

    # 按数据量自适应选择模型
    SMALL_DATA_MODELS = ['ar1', 'exp_smoothing', 'trend', 'moving_avg', 'linear']
    MEDIUM_DATA_MODELS = ['linear', 'ridge', 'ar1', 'exp_smoothing', 'random_forest']
    LARGE_DATA_MODELS = ['linear', 'ridge', 'random_forest', 'gradient_boosting',
                         'ar1', 'exp_smoothing']

    def __init__(self, config=None):
        self.config = config or {}
        self.models = {}
        self.predictions = {}
        self.metrics = {}

    # ================================================================
    # 多窗口数据聚合
    # ================================================================

    @staticmethod
    def _aggregate_with_window(df, window_seconds):
        """按指定时间窗口聚合 IO 数据"""
        df = df.copy()
        df['time_window'] = df['datetime'].dt.floor(f'{window_seconds}s')
        df['size_kb'] = df['size'] / 1024.0

        ts = df.groupby('time_window').agg(
            iops=('operation', 'count'),
            read_ops=('operation', lambda x: (x == 'R').sum()),
            write_ops=('operation', lambda x: (x == 'W').sum()),
            throughput_kb=('size_kb', 'sum'),
            avg_request_size_kb=('size_kb', 'mean'),
            distinct_devices=('device_id', 'nunique'),
        ).reset_index()

        ts['iops_read'] = ts['read_ops']
        ts['iops_write'] = ts['write_ops']
        ts = ts.rename(columns={'time_window': 'datetime'})
        return ts

    @staticmethod
    def _select_best_window(df, min_points=30):
        """
        自动选择最优时间窗口
        目标：在 min_points 附近获得最多数据点
        """
        windows = [0.2, 0.5, 1.0, 2.0]
        for w in windows:
            ts = TimeSeriesPredictor._aggregate_with_window(df, w)
            n = len(ts)
            logger.info(f"  窗口 {w}s -> {n} 个数据点")
            if n >= min_points:
                logger.info(f"  选择窗口 {w}s (获得 {n} 个数据点)")
                return w, ts

        # 取最大窗口
        best_w = max(windows)
        ts = TimeSeriesPredictor._aggregate_with_window(df, best_w)
        logger.info(f"  数据量有限，使用最大窗口 {best_w}s (获得 {len(ts)} 个数据点)")
        return best_w, ts

    # ================================================================
    # 特征工程
    # ================================================================

    def _build_features(self, df_features, target_series, n_lags=5):
        """
        构建丰富的特征矩阵

        包含：滞后特征、滑动统计、差分特征、变化率、时间特征
        """
        df = df_features.copy()

        # 目标变量的滞后特征
        for i in range(1, n_lags + 1):
            df[f'target_lag_{i}'] = target_series.shift(i).values

        # 滑动统计
        for w in [2, 3, 5]:
            if len(df) >= w:
                df[f'rolling_mean_{w}'] = target_series.rolling(window=w, min_periods=1).mean().values
                df[f'rolling_std_{w}'] = target_series.rolling(window=w, min_periods=1).std().fillna(0).values
                df[f'rolling_min_{w}'] = target_series.rolling(window=w, min_periods=1).min().values
                df[f'rolling_max_{w}'] = target_series.rolling(window=w, min_periods=1).max().values

        # 差分特征（变化量）
        df['diff_1'] = target_series.diff().fillna(0).values
        df['diff_2'] = target_series.diff(2).fillna(0).values

        # 变化率（相对变化）
        with np.errstate(divide='ignore', invalid='ignore'):
            rate = (target_series.diff() / target_series.shift(1).replace(0, np.nan)).fillna(0)
            df['pct_change'] = rate.values

        # EWM（指数加权滑动平均）
        for span in [3, 5]:
            df[f'ewm_{span}'] = target_series.ewm(span=span, adjust=False).mean().values

        # 趋势强度（线性拟合斜率）
        def trend_strength(s):
            if len(s) < 3:
                return 0.0
            x = np.arange(len(s))
            return np.polyfit(x, s, 1)[0]

        if len(df) >= 3:
            trend_vals = []
            for i in range(len(df)):
                if i < 2:
                    trend_vals.append(0.0)
                else:
                    window_data = target_series.iloc[max(0, i-2):i+1].values
                    trend_vals.append(trend_strength(window_data))
            df['trend_strength'] = trend_vals

        # 时间特征
        if isinstance(df.index, pd.DatetimeIndex):
            df['hour'] = df.index.hour
            df['minute'] = df.index.minute
            df['second'] = df.index.second
            df['total_seconds'] = (df.index - df.index[0]).total_seconds()

        df = df.dropna()
        y = target_series.loc[df.index]

        feature_cols = [c for c in df.columns if c != 'target']
        X = df[feature_cols]

        return X, y, feature_cols

    def _build_multistep_data(self, df_features, target_series, n_lags=5, predict_steps=2):
        """构建多步预测数据集"""
        X, y, feature_cols = self._build_features(df_features, target_series, n_lags)

        if len(X) < predict_steps + n_lags:
            raise ValueError(
                f"数据不足: 需要 {predict_steps + n_lags} 个样本, 实际 {len(X)} 个"
            )

        X_train = X.iloc[:-predict_steps]
        y_train = y.iloc[:-predict_steps]
        X_test = X.iloc[-predict_steps:]
        y_test = y.iloc[-predict_steps:]
        train_index = X_train.index
        test_index = X_test.index

        return X_train, y_train, X_test, y_test, train_index, test_index, feature_cols

    # ================================================================
    # 模型工厂
    # ================================================================

    def _create_model(self, model_name, data_size=None):
        """创建指定类型的模型"""
        if model_name == 'linear':
            return LinearRegression()
        elif model_name == 'ridge':
            return Ridge(alpha=1.0 if data_size and data_size < 20 else 0.1)
        elif model_name == 'lasso':
            return Lasso(alpha=0.1, max_iter=5000)
        elif model_name == 'random_forest':
            n = max(10, min(100, data_size // 2)) if data_size else 50
            return RandomForestRegressor(
                n_estimators=n,
                max_depth=min(5, max(2, data_size // 10)) if data_size else 4,
                min_samples_leaf=max(1, data_size // 20) if data_size else 2,
                random_state=42, n_jobs=-1
            )
        elif model_name == 'gradient_boosting':
            n = max(10, min(50, data_size // 3)) if data_size else 30
            return GradientBoostingRegressor(
                n_estimators=n, max_depth=3, learning_rate=0.1,
                min_samples_leaf=max(1, data_size // 20) if data_size else 2,
                random_state=42
            )
        elif model_name == 'exp_smoothing':
            return ExponentialSmoothingForecaster(alpha=0.3)
        elif model_name == 'ar1':
            return AR1Forecaster()
        elif model_name == 'trend':
            return TrendForecaster(poly_degree=1)
        elif model_name == 'moving_avg':
            return MovingAverageForecaster(window=3)
        else:
            raise ValueError(f"不支持的模型: {model_name}")

    # ================================================================
    # 递归多步预测
    # ================================================================

    def _recursive_predict(self, model, X_train, y_train, X_test, feature_cols,
                           df_features_template, target_series, predict_steps,
                           model_name, is_sklearn_model=True):
        """
        递归多步预测：
        每步预测后，将预测值作为特征重新构建下一步的输入
        """
        preds = []
        current_X = X_train.copy()
        current_y = list(y_train.values)
        last_idx = X_train.index[-1]
        df_template = df_features_template.copy()

        for step in range(predict_steps):
            if is_sklearn_model:
                model.fit(current_X, current_y)
                pred = model.predict(X_test.iloc[[step]])[0]
            else:
                # 非 sklearn 模型（简单统计模型）直接用全部训练数据拟合
                model.fit(current_y)
                pred = model.predict(1)[0]

            preds.append(max(0, pred))  # 确保非负

            # 递推：将预测值加入训练序列
            current_y.append(pred)

            if is_sklearn_model and step < predict_steps - 1:
                # 更新 X 以便下一步预测（递推更新滞后特征）
                current_X = self._update_X_for_recursive(
                    current_X, current_y, feature_cols, df_template
                )

        return np.array(preds)

    def _update_X_for_recursive(self, X_base, all_y, feature_cols, df_template):
        """递推更新特征矩阵"""
        y_arr = np.array(all_y)
        n_lags = len([c for c in feature_cols if 'lag' in c])

        rows = []
        for i in range(len(y_arr) - n_lags):
            row = {}
            for lag in range(1, n_lags + 1):
                if i + lag < len(y_arr):
                    row[f'target_lag_{lag}'] = y_arr[i + lag]
                else:
                    row[f'target_lag_{lag}'] = y_arr[-1]

            for w in [2, 3, 5]:
                if f'rolling_mean_{w}' in feature_cols:
                    win = y_arr[max(0, i - w + 1):i + 1]
                    row[f'rolling_mean_{w}'] = np.mean(win) if len(win) > 0 else 0
                    row[f'rolling_std_{w}'] = np.std(win) if len(win) > 0 else 0
                    row[f'rolling_min_{w}'] = np.min(win) if len(win) > 0 else 0
                    row[f'rolling_max_{w}'] = np.max(win) if len(win) > 0 else 0

            if 'diff_1' in feature_cols and i > 0:
                row['diff_1'] = y_arr[i] - y_arr[i - 1]
            if 'diff_2' in feature_cols and i > 1:
                row['diff_2'] = y_arr[i] - y_arr[i - 2]

            if 'pct_change' in feature_cols and i > 0 and y_arr[i - 1] != 0:
                row['pct_change'] = (y_arr[i] - y_arr[i - 1]) / y_arr[i - 1]

            for span in [3, 5]:
                if f'ewm_{span}' in feature_cols:
                    row[f'ewm_{span}'] = pd.Series(y_arr[:i + 1]).ewm(span=span, adjust=False).mean().iloc[-1]

            row_vals = [row.get(c, 0) for c in feature_cols]
            rows.append(row_vals)

        return pd.DataFrame(rows, columns=feature_cols)

    # ================================================================
    # 训练和预测
    # ================================================================

    def train_and_predict(self, df_raw_io, target_col='iops',
                         model_names=None, n_lags=None, predict_steps=None):
        """
        使用多种模型训练并预测（递归多步）

        Args:
            df_raw_io: 原始 I/O DataFrame（来自 parse_io_traces）
            target_col: 预测目标
            model_names: 模型列表
            n_lags: 滞后阶数（自动选择）
            predict_steps: 预测步数（自动选择）
        """
        # ---- 1. 选择最优时间窗口 ----
        if n_lags is None:
            n_lags = 5
        if predict_steps is None:
            predict_steps = 2
        if model_names is None:
            model_names = self.MEDIUM_DATA_MODELS

        logger.info(f"  选择时间窗口...")
        window_sec, ts = self._select_best_window(df_raw_io, min_points=15)

        if target_col not in ts.columns:
            raise ValueError(f"目标列 '{target_col}' 不在: {list(ts.columns)}")

        # ---- 2. 自适应调整 n_lags ----
        n_lags = min(n_lags, max(2, len(ts) // 5))
        if predict_steps is not None:
            predict_steps = min(predict_steps, max(1, len(ts) // 8))

        logger.info(f"  训练集: 目标={target_col}, 窗口={window_sec}s, "
                     f"n_lags={n_lags}, predict_steps={predict_steps}, "
                     f"数据点={len(ts)}")

        # ---- 3. 构建特征 ----
        series = ts.set_index('datetime')[target_col]
        feature_df = pd.DataFrame(index=series.index)
        feature_df['target'] = series

        try:
            X_train, y_train, X_test, y_test, train_idx, test_idx, feature_cols = \
                self._build_multistep_data(
                    feature_df, series, n_lags=n_lags, predict_steps=predict_steps
                )
        except ValueError as e:
            logger.warning(f"  特征构建失败: {e}")
            raise

        logger.info(f"  实际训练样本: {len(X_train)}, 测试样本: {len(X_test)}")

        # ---- 4. 训练各模型 ----
        results = {
            'target': target_col,
            'window_seconds': window_sec,
            'train_index': train_idx,
            'test_index': test_idx,
            'y_train': y_train,
            'y_test': y_test,
            'actual_values': y_test.values,
            'models': {}
        }

        sklearn_models = ['linear', 'ridge', 'lasso', 'random_forest', 'gradient_boosting']

        for model_name in model_names:
            try:
                is_sklearn = model_name in sklearn_models
                model = self._create_model(
                    model_name,
                    data_size=len(X_train) if is_sklearn else len(y_train)
                )

                if is_sklearn:
                    # 标准化
                    scaler = StandardScaler()
                    X_train_scaled = scaler.fit_transform(X_train)
                    X_test_scaled = scaler.transform(X_test)
                    model.fit(X_train_scaled, y_train)
                    y_pred = model.predict(X_test_scaled)
                else:
                    y_pred = self._recursive_predict(
                        model, X_train, y_train, X_test, feature_cols,
                        feature_df, series, predict_steps, model_name,
                        is_sklearn_model=False
                    )

                y_pred = np.maximum(y_pred, 0)
                metrics = self._calc_metrics(y_test.values, y_pred)

                results['models'][model_name] = {
                    'model': model,
                    'predictions': y_pred,
                    'metrics': metrics
                }

                logger.info(
                    f"  [{model_name:20s}] "
                    f"MAE={metrics['MAE']:.2f}  "
                    f"RMSE={metrics['RMSE']:.2f}  "
                    f"R2={metrics['R2']:.4f}  "
                    f"MAPE={metrics['MAPE']:.2f}%  "
                    f"Acc={metrics['Accuracy']:.1f}%"
                )

            except Exception as e:
                logger.warning(f"  [{model_name}] 失败: {e}")
                results['models'][model_name] = {'error': str(e)}

        self.predictions[target_col] = results
        return results

    # ================================================================
    # 回归指标
    # ================================================================

    @staticmethod
    def _calc_metrics(y_true, y_pred):
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)
        mask = y_true != 0
        if mask.sum() > 0:
            mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
        else:
            mape = float('inf')
        accuracy = max(0, 100 - mape)

        return {
            'MAE': round(mae, 4),
            'RMSE': round(rmse, 4),
            'R2': round(r2, 4),
            'MAPE': round(mape, 2),
            'Accuracy': round(accuracy, 2),
            'y_true': y_true,
            'y_pred': y_pred
        }

    # ================================================================
    # 多指标批量预测
    # ================================================================

    def run_multi_target_prediction(self, df_raw_io, targets=None,
                                   model_names=None, n_lags=None,
                                   predict_steps=None):
        """对多个目标指标进行预测"""
        if targets is None:
            targets = ['iops', 'throughput_kb']

        all_results = {}
        for target in targets:
            logger.info(f"\n{'='*55}")
            logger.info(f"预测目标: {target}")
            logger.info(f"{'='*55}")
            try:
                result = self.train_and_predict(
                    df_raw_io, target_col=target,
                    model_names=model_names,
                    n_lags=n_lags,
                    predict_steps=predict_steps
                )
                all_results[target] = result
            except Exception as e:
                logger.error(f"预测 '{target}' 失败: {e}")
                all_results[target] = {'error': str(e)}

        return all_results

    # ================================================================
    # 可视化（与 v1 相同）
    # ================================================================

    def visualize_predictions(self, all_results, ts_df, save_dir):
        os.makedirs(save_dir, exist_ok=True)
        self._plot_prediction_comparison(all_results, save_dir)
        self._plot_metrics_summary(all_results, save_dir)
        self._plot_residuals(all_results, save_dir)
        self._plot_dashboard(all_results, save_dir)
        self._plot_regression_scatter(all_results, save_dir)
        self._plot_model_comparison_bar(all_results, save_dir)
        logger.info(f"可视化图表已保存至: {save_dir}")

    def _plot_prediction_comparison(self, all_results, save_dir):
        """预测 vs 实际对比折线图"""
        valid_results = {k: v for k, v in all_results.items() if 'models' in v}
        if not valid_results:
            return

        target_labels = {
            'iops': '总 IOPS', 'iops_read': '读 IOPS', 'iops_write': '写 IOPS',
            'throughput_kb': '吞吐量 (KB/s)', 'avg_request_size_kb': '平均请求大小 (KB)',
            'distinct_devices': '活跃设备数',
        }

        model_styles = {
            'linear':            {'color': '#e74c3c', 'marker': 'D', 'ls': '--'},
            'ridge':             {'color': '#3498db', 'marker': '^', 'ls': '-.'},
            'lasso':             {'color': '#9b59b6', 'marker': 'p', 'ls': ':'},
            'random_forest':     {'color': '#2ecc71', 'marker': 's', 'ls': '--'},
            'gradient_boosting': {'color': '#f39c12', 'marker': 'v', 'ls': '-.'},
            'exp_smoothing':     {'color': '#1abc9c', 'marker': 'o', 'ls': '-'},
            'ar1':               {'color': '#e91e63', 'marker': '*', 'ls': '--'},
            'trend':             {'color': '#00bcd4', 'marker': 'h', 'ls': '-.'},
            'moving_avg':        {'color': '#8bc34a', 'marker': 'd', 'ls': ':'},
        }
        default_colors = plt.cm.tab10(np.linspace(0, 1, 10))

        n_targets = len(valid_results)
        fig, axes = plt.subplots(n_targets, 1, figsize=(18, 6.5 * n_targets))
        if n_targets == 1:
            axes = [axes]

        for ax, (target, result) in zip(axes, valid_results.items()):
            y_train = result['y_train']
            y_test = result['y_test']
            train_idx = result['train_index']
            test_idx = result['test_index']
            actual_test = y_test.values

            # 历史负载（蓝色）
            ax.plot(train_idx, y_train.values, '-', color='#2980b9', linewidth=1.8,
                    alpha=0.7, label='历史负载', zorder=2)

            # 预测区间实际值（黑色粗实线）
            ax.plot(test_idx, actual_test, '-o', color='#2c3e50', linewidth=2.5,
                    markersize=10, markerfacecolor='white', markeredgewidth=2,
                    label='实际负载（预测区间）', zorder=5)

            # 各模型预测线
            models_data = [(n, d) for n, d in result['models'].items() if 'predictions' in d]
            for i, (model_name, data) in enumerate(models_data):
                pred = data['predictions']
                style = model_styles.get(model_name, {})
                color = style.get('color', default_colors[i % 10])
                marker = style.get('marker', 'D')
                ls = style.get('ls', '--')
                m = data['metrics']
                ax.plot(test_idx, pred, ls, color=color, linewidth=2.0,
                        marker=marker, markersize=7, markeredgecolor=color,
                        markerfacecolor='white', markeredgewidth=1.5,
                        label=f'{model_name.replace("_", " ").title()} '
                              f'(R2={m["R2"]:.3f}, MAPE={m["MAPE"]:.1f}%)',
                        zorder=4)

            # 分界线
            if len(test_idx) > 0:
                boundary = test_idx[0]
                ax.axvline(x=boundary, color='#95a5a6', linestyle=':', linewidth=1.5, alpha=0.8)
                y_max = max(y_train.max(), actual_test.max()) * 1.05
                ax.text(boundary, y_max * 0.93, '  训练 | 预测  ',
                        fontsize=10, color='#7f8c8d', fontweight='bold',
                        ha='left', va='top',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='#ecf0f1',
                                  edgecolor='#bdc3c7', alpha=0.9))

            # 最佳模型偏差填充
            if models_data:
                best = min(models_data, key=lambda x: x[1]['metrics']['RMSE'])
                best_pred = best[1]['predictions']
                ax.fill_between(test_idx, actual_test, best_pred,
                                alpha=0.12, color='#27ae60', zorder=1)

            # 数值标注
            for j, (idx_val, actual_val) in enumerate(zip(test_idx, actual_test)):
                ax.annotate(f'{actual_val:.1f}', (idx_val, actual_val),
                            textcoords="offset points", xytext=(0, 14),
                            ha='center', fontsize=9, fontweight='bold', color='#2c3e50',
                            bbox=dict(boxstyle='round,pad=0.15', facecolor='white',
                                      edgecolor='#2c3e50', alpha=0.85))
                for k, (model_name, data) in enumerate(models_data):
                    pred_val = data['predictions'][j]
                    style = model_styles.get(model_name, {})
                    color = style.get('color', default_colors[k % 10])
                    offset_x = (k - len(models_data) / 2 + 0.5) * 25
                    ax.annotate(f'{pred_val:.1f}', (idx_val, pred_val),
                                textcoords="offset points",
                                xytext=(offset_x, -18 - (k % 2) * 12),
                                ha='center', fontsize=7, color=color, fontweight='bold')

            ylabel = target_labels.get(target, target)
            ax.set_title(f'{ylabel} — 历史负载 + 预测对比 (窗口 {result.get("window_seconds", "?")}s)',
                         fontsize=15, fontweight='bold')
            ax.set_xlabel('时间', fontsize=12)
            ax.set_ylabel(ylabel, fontsize=12)
            ax.legend(fontsize=8.5, loc='upper left', framealpha=0.9, ncol=2)
            ax.grid(True, alpha=0.25, linestyle='--')
            ax.set_facecolor('#fafafa')

            all_vals = list(y_train.values) + list(actual_test)
            for _, d in models_data:
                all_vals += list(d['predictions'])
            y_lo, y_hi = min(all_vals), max(all_vals)
            margin = (y_hi - y_lo) * 0.15 if y_hi != y_lo else 1
            ax.set_ylim(y_lo - margin, y_hi + margin * 2.5)

        plt.tight_layout(pad=2.0)
        filepath = os.path.join(save_dir, 'prediction_comparison.png')
        fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        logger.info(f"  保存: {filepath}")

    def _plot_metrics_summary(self, all_results, save_dir):
        """回归分析指标汇总"""
        valid_results = {k: v for k, v in all_results.items() if 'models' in v}
        if not valid_results:
            return

        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        metrics_names = ['MAE', 'RMSE', 'R2']
        metric_titles = ['平均绝对误差 (MAE)', '均方根误差 (RMSE)', '决定系数 (R2)']

        for ax, metric_name, title in zip(axes, metrics_names, metric_titles):
            records = []
            for target, result in valid_results.items():
                for mn, md in result['models'].items():
                    if 'metrics' in md:
                        records.append({
                            'Model': mn.replace('_', ' ').title(),
                            'Target': target.replace('_', ' ').title(),
                            'Value': md['metrics'][metric_name]
                        })
            if not records:
                continue
            df = pd.DataFrame(records)
            colors = plt.cm.Set2(np.linspace(0, 1, len(df)))
            bars = ax.bar(range(len(df)), df['Value'], color=colors, edgecolor='white')
            ax.set_xticks(range(len(df)))
            ax.set_xticklabels([f"{r['Model']}\n({r['Target']})" for _, r in df.iterrows()],
                               fontsize=8, rotation=30, ha='right')
            ax.set_title(title, fontsize=13, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='y')
            for bar, val in zip(bars, df['Value']):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                        f'{val:.2f}', ha='center', va='bottom', fontsize=8)

        plt.suptitle('回归分析指标汇总', fontsize=16, fontweight='bold', y=1.02)
        plt.tight_layout()
        filepath = os.path.join(save_dir, 'metrics_summary.png')
        fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        logger.info(f"  保存: {filepath}")

    def _plot_residuals(self, all_results, save_dir):
        """残差分析"""
        valid_results = {k: v for k, v in all_results.items() if 'models' in v}
        if not valid_results:
            return

        fig, axes = plt.subplots(1, 2, figsize=(16, 6))

        residuals_all, labels = [], []
        mape_all, mape_labels = [], []
        for target, result in valid_results.items():
            for mn, data in result['models'].items():
                if 'metrics' not in data:
                    continue
                m = data['metrics']
                residuals_all.append(m['y_true'] - m['y_pred'])
                labels.append(f"{mn}\n({target})")
                mape_all.append(m['MAPE'])
                mape_labels.append(f"{mn}\n({target})")

        if residuals_all:
            bp = axes[0].boxplot(residuals_all, labels=labels, patch_artist=True, showmeans=True)
            colors = plt.cm.Pastel1(np.linspace(0, 1, len(residuals_all)))
            for p, c in zip(bp['boxes'], colors):
                p.set_facecolor(c)
            axes[0].axhline(y=0, color='red', linestyle='--', alpha=0.7)
            axes[0].set_title('残差分布 (预测 - 实际)', fontsize=13, fontweight='bold')
            axes[0].set_ylabel('残差', fontsize=12)
            axes[0].grid(True, alpha=0.3)

        if mape_all:
            colors = ['#27ae60' if v < 10 else '#f39c12' if v < 30 else '#e74c3c' for v in mape_all]
            bars = axes[1].barh(range(len(mape_all)), mape_all, color=colors, edgecolor='white')
            for i, (bar, val) in enumerate(zip(bars, mape_all)):
                axes[1].text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                             f'{val:.1f}%', va='center', fontsize=9)
            axes[1].set_yticks(range(len(mape_labels)))
            axes[1].set_yticklabels(mape_labels, fontsize=8)
            axes[1].axvline(x=10, color='green', linestyle='--', alpha=0.7, label='<10% 优秀')
            axes[1].axvline(x=30, color='orange', linestyle='--', alpha=0.7, label='<30% 良好')
            axes[1].set_title('平均绝对百分比误差 (MAPE)', fontsize=13, fontweight='bold')
            axes[1].set_xlabel('MAPE (%)', fontsize=12)
            axes[1].legend(fontsize=9)
            axes[1].grid(True, alpha=0.3, axis='x')

        plt.tight_layout()
        filepath = os.path.join(save_dir, 'residual_analysis.png')
        fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        logger.info(f"  保存: {filepath}")

    def _plot_model_comparison_bar(self, all_results, save_dir):
        """模型准确率对比柱状图"""
        valid_results = {k: v for k, v in all_results.items() if 'models' in v}
        if not valid_results:
            return

        fig, ax = plt.subplots(figsize=(14, 7))

        model_acc = {}
        for target, result in valid_results.items():
            for mn, data in result['models'].items():
                if 'metrics' in data:
                    key = mn.replace('_', ' ').title()
                    if key not in model_acc:
                        model_acc[key] = []
                    model_acc[key].append(data['metrics']['Accuracy'])

        model_names = list(model_acc.keys())
        avg_acc = [np.mean(v) for v in model_acc.values()]
        colors = ['#27ae60' if a >= 90 else '#f39c12' if a >= 70 else '#e74c3c'
                  for a in avg_acc]
        bars = ax.bar(model_names, avg_acc, color=colors, edgecolor='white', linewidth=1.5)

        for bar, val in zip(bars, avg_acc):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f'{val:.1f}%', ha='center', va='bottom', fontsize=12, fontweight='bold')

        ax.axhline(y=90, color='green', linestyle='--', alpha=0.6, label='优秀 (>=90%)')
        ax.axhline(y=70, color='orange', linestyle='--', alpha=0.6, label='良好 (>=70%)')
        ax.axhline(y=50, color='red', linestyle='--', alpha=0.6, label='较差 (<50%)')

        ax.set_title('模型预测准确率对比', fontsize=16, fontweight='bold')
        ax.set_ylabel('平均准确率 (%)', fontsize=12)
        ax.set_ylim(0, 105)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')
        plt.xticks(fontsize=11)
        plt.tight_layout()

        filepath = os.path.join(save_dir, 'model_comparison.png')
        fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        logger.info(f"  保存: {filepath}")

    def _plot_dashboard(self, all_results, save_dir):
        """综合仪表盘"""
        valid_results = {k: v for k, v in all_results.items() if 'models' in v}
        if not valid_results:
            return

        target_labels = {
            'iops': '总 IOPS', 'iops_read': '读 IOPS', 'iops_write': '写 IOPS',
            'throughput_kb': '吞吐量 (KB/s)',
        }

        fig = plt.figure(figsize=(22, 16))
        fig.suptitle('I/O 负载预测分析仪表盘 v2', fontsize=20, fontweight='bold', y=0.99)

        gs = fig.add_gridspec(4, 3, hspace=0.40, wspace=0.35, height_ratios=[1.3, 1, 1, 0.8])

        # ---- 顶部：完整历史 + 预测折线图 ----
        ax_main = fig.add_subplot(gs[0, :])
        target = list(valid_results.keys())[0]
        result = valid_results[target]
        y_train, y_test = result['y_train'], result['y_test']
        train_idx, test_idx = result['train_index'], result['test_index']
        actual = y_test.values

        model_styles_dash = {
            'linear': '#e74c3c', 'ridge': '#3498db', 'lasso': '#9b59b6',
            'random_forest': '#2ecc71', 'gradient_boosting': '#f39c12',
            'exp_smoothing': '#1abc9c', 'ar1': '#e91e63', 'trend': '#00bcd4',
        }

        ax_main.plot(train_idx, y_train.values, '-', color='#2980b9', linewidth=1.8,
                     alpha=0.7, label='历史负载')
        ax_main.plot(test_idx, actual, '-o', color='#2c3e50', linewidth=2.5,
                     markersize=10, markerfacecolor='white', markeredgewidth=2,
                     label='实际负载', zorder=5)

        for mn, data in result['models'].items():
            if 'predictions' not in data:
                continue
            color = model_styles_dash.get(mn, '#999')
            m = data['metrics']
            ax_main.plot(test_idx, data['predictions'], '--',
                         color=color, linewidth=2.0,
                         marker='D', markersize=7, markeredgecolor=color,
                         markerfacecolor='white', markeredgewidth=1.5,
                         label=f"{mn.replace('_',' ').title()} "
                               f"(Acc={m['Accuracy']:.0f}%, R2={m['R2']:.3f})",
                         zorder=4)

        ax_main.axvline(x=test_idx[0], color='#95a5a6', linestyle=':', linewidth=1.5)
        ax_main.fill_between(test_idx, actual,
                             result['models'][min(result['models'], key=lambda x: result['models'][x]['metrics']['RMSE'] if 'metrics' in result['models'][x] else float('inf'))]['predictions'],
                             alpha=0.10, color='#27ae60')

        for j, (idx_val, actual_val) in enumerate(zip(test_idx, actual)):
            ax_main.annotate(f'{actual_val:.1f}', (idx_val, actual_val),
                             textcoords="offset points", xytext=(0, 14),
                             ha='center', fontsize=9, fontweight='bold', color='#2c3e50',
                             bbox=dict(boxstyle='round,pad=0.15', facecolor='white',
                                       edgecolor='#2c3e50', alpha=0.85))

        ylabel = target_labels.get(target, target)
        ax_main.set_title(f'时间序列预测 — {ylabel}', fontsize=14, fontweight='bold')
        ax_main.set_xlabel('时间', fontsize=12)
        ax_main.set_ylabel(ylabel, fontsize=12)
        ax_main.legend(fontsize=9, loc='upper left', ncol=3, framealpha=0.9)
        ax_main.grid(True, alpha=0.25, linestyle='--')
        ax_main.set_facecolor('#fafafa')

        # ---- 指标表 ----
        ax_table = fig.add_subplot(gs[1, :])
        ax_table.axis('off')
        table_data = []
        for tgt, res in valid_results.items():
            for mn, md in res['models'].items():
                if 'metrics' not in md:
                    continue
                m = md['metrics']
                table_data.append([
                    tgt.replace('_', ' ').title()[:12],
                    mn.replace('_', ' ').title()[:14],
                    f"{m['MAE']:.2f}", f"{m['RMSE']:.2f}",
                    f"{m['R2']:.4f}", f"{m['MAPE']:.1f}%",
                    f"{m['Accuracy']:.1f}%"
                ])

        if table_data:
            col_labels = ['指标', '模型', 'MAE', 'RMSE', 'R2', 'MAPE', '准确率']
            table = ax_table.table(
                cellText=table_data, colLabels=col_labels,
                cellLoc='center', loc='center',
                colWidths=[0.16, 0.18, 0.12, 0.12, 0.12, 0.12, 0.14]
            )
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1.1, 1.5)
            for j in range(len(col_labels)):
                table[0, j].set_facecolor('#34495e')
                table[0, j].set_text_props(color='white', fontweight='bold')
        ax_table.set_title('回归分析结果汇总', fontsize=12, fontweight='bold')

        # ---- MAPE 对比 ----
        ax_mape = fig.add_subplot(gs[2, 0])
        mape_data, mape_labels = [], []
        for tgt, res in valid_results.items():
            for mn, md in res['models'].items():
                if 'metrics' in md:
                    mape_data.append(md['metrics']['MAPE'])
                    mape_labels.append(f"{mn[:10]}\n({tgt[:8]})")
        if mape_data:
            colors_m = ['#27ae60' if v < 10 else '#f39c12' if v < 30 else '#e74c3c' for v in mape_data]
            bars = ax_mape.barh(range(len(mape_data)), mape_data, color=colors_m)
            ax_mape.set_yticks(range(len(mape_labels)))
            ax_mape.set_yticklabels(mape_labels, fontsize=8)
            ax_mape.set_title('MAPE (%)', fontsize=12, fontweight='bold')
            ax_mape.grid(True, alpha=0.3, axis='x')

        # ---- R2 柱状图 ----
        ax_r2 = fig.add_subplot(gs[2, 1])
        r2_data, r2_labels = [], []
        for tgt, res in valid_results.items():
            for mn, md in res['models'].items():
                if 'metrics' in md:
                    r2_data.append(md['metrics']['R2'])
                    r2_labels.append(f"{mn[:10]}\n({tgt[:8]})")
        if r2_data:
            bars = ax_r2.bar(range(len(r2_data)), r2_data,
                             color=plt.cm.RdYlGn(np.clip(r2_data, 0, 1)))
            ax_r2.set_xticks(range(len(r2_labels)))
            ax_r2.set_xticklabels(r2_labels, fontsize=7, rotation=30, ha='right')
            ax_r2.axhline(y=0.8, color='green', linestyle='--', alpha=0.5, label='>=0.8 优秀')
            ax_r2.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            ax_r2.set_title('R2 决定系数', fontsize=12, fontweight='bold')
            ax_r2.set_ylim(min(0, min(r2_data) - 0.1), 1.05)
            ax_r2.legend(fontsize=8)
            ax_r2.grid(True, alpha=0.3, axis='y')

        # ---- 准确率 ----
        ax_acc = fig.add_subplot(gs[2, 2])
        acc_data, acc_labels = [], []
        for tgt, res in valid_results.items():
            for mn, md in res['models'].items():
                if 'metrics' in md:
                    acc_data.append(md['metrics']['Accuracy'])
                    acc_labels.append(f"{mn[:10]}\n({tgt[:8]})")
        if acc_data:
            colors_a = ['#27ae60' if v >= 90 else '#f39c12' if v >= 70 else '#e74c3c'
                        for v in acc_data]
            bars = ax_acc.bar(range(len(acc_data)), acc_data, color=colors_a)
            ax_acc.set_xticks(range(len(acc_labels)))
            ax_acc.set_xticklabels(acc_labels, fontsize=7, rotation=30, ha='right')
            ax_acc.axhline(y=90, color='green', linestyle='--', alpha=0.5)
            ax_acc.axhline(y=70, color='orange', linestyle='--', alpha=0.5)
            ax_acc.set_title('预测准确率 (%)', fontsize=12, fontweight='bold')
            ax_acc.set_ylim(0, 105)
            ax_acc.grid(True, alpha=0.3, axis='y')

        filepath = os.path.join(save_dir, 'prediction_dashboard.png')
        fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        logger.info(f"  保存: {filepath}")

    def _plot_regression_scatter(self, all_results, save_dir):
        """回归散点图"""
        valid_results = {k: v for k, v in all_results.items() if 'models' in v}
        if not valid_results:
            return

        n_total = sum(sum(1 for d in r['models'].values() if 'metrics' in d)
                       for r in valid_results.values())
        if n_total == 0:
            return

        ncols = min(4, n_total)
        nrows = (n_total + ncols - 1) // ncols
        fig, axes = plt.subplots(nrows, ncols, figsize=(5.5 * ncols, 5 * nrows))
        if nrows == 1 and ncols == 1:
            axes = np.array([[axes]])
        axes = np.atleast_2d(axes)

        idx = 0
        for target, result in valid_results.items():
            for model_name, data in result['models'].items():
                if 'metrics' not in data:
                    continue
                r, c = idx // ncols, idx % ncols
                ax = axes[r, c]
                idx += 1

                y_t, y_p = data['metrics']['y_true'], data['metrics']['y_pred']
                ax.scatter(y_t, y_p, s=100, alpha=0.7, edgecolors='white', linewidth=1.5, zorder=3)

                v_min = min(y_t.min(), y_p.min()) * 0.9
                v_max = max(y_t.max(), y_p.max()) * 1.1
                ax.plot([v_min, v_max], [v_min, v_max], 'r--', lw=2, label='完美拟合', zorder=2)

                if len(y_t) >= 2:
                    z = np.polyfit(y_t, y_p, 1)
                    ax.plot(np.linspace(v_min, v_max, 100),
                            np.polyval(z, np.linspace(v_min, v_max, 100)),
                            'g-', lw=1.5, alpha=0.7, label=f'拟合 (y={z[0]:.2f}x+{z[1]:.1f})')

                m = data['metrics']
                ax.set_title(f"{model_name.replace('_',' ').title()}\n"
                             f"{target.replace('_',' ').title()}",
                             fontsize=10, fontweight='bold')
                ax.set_xlabel('实际值', fontsize=9)
                ax.set_ylabel('预测值', fontsize=9)
                ax.text(0.05, 0.95,
                        f"R2={m['R2']:.4f}\nMAE={m['MAE']:.2f}\nMAPE={m['MAPE']:.1f}%\nAcc={m['Accuracy']:.1f}%",
                        transform=ax.transAxes, fontsize=8, va='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
                ax.legend(fontsize=7, loc='lower right')
                ax.grid(True, alpha=0.3)

        for i in range(idx, nrows * ncols):
            axes[i // ncols, i % ncols].set_visible(False)

        plt.suptitle('回归散点分析', fontsize=15, fontweight='bold')
        plt.tight_layout()
        filepath = os.path.join(save_dir, 'regression_scatter.png')
        fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        logger.info(f"  保存: {filepath}")

    # ================================================================
    # 报告打印
    # ================================================================

    def print_report(self, all_results):
        print("\n" + "=" * 72)
        print("           I/O 负载预测分析报告 v2")
        print("=" * 72)

        for target, result in all_results.items():
            if 'models' not in result:
                if 'error' in result:
                    print(f"\n[{target}] 预测失败: {result['error']}")
                continue

            print(f"\n{'─'*65}")
            print(f"  预测指标: {target}  |  时间窗口: {result.get('window_seconds', '?')}s")
            print(f"  实际值:   {result['actual_values']}")
            print(f"{'─'*65}")
            print(f"  {'模型':<22} {'MAE':<10} {'RMSE':<10} {'R2':<10} {'MAPE':<10} {'准确率':<10}")
            print(f"  {'─'*72}")

            best_model, best_rmse = None, float('inf')
            for mn, data in result['models'].items():
                if 'metrics' not in data:
                    continue
                m = data['metrics']
                print(f"  {mn:<22} {m['MAE']:<10.4f} {m['RMSE']:<10.4f} "
                      f"{m['R2']:<10.4f} {m['MAPE']:<10.2f} {m['Accuracy']:<10.2f}")
                if m['RMSE'] < best_rmse:
                    best_rmse, best_model = m['RMSE'], mn

            if best_model:
                bm = result['models'][best_model]
                print(f"\n  ★ 最佳模型: {best_model} (RMSE={best_rmse:.4f}, "
                      f"准确率={bm['metrics']['Accuracy']:.1f}%)")
                print(f"    预测值: {bm['predictions']}")
                print(f"    实际值: {result['actual_values']}")
                print(f"    偏差:   {bm['predictions'] - result['actual_values']}")

        print("\n" + "=" * 72)
