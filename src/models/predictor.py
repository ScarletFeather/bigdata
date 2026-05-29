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

        # 滑动统计（shift(1) 确保只用历史数据，避免数据泄露）
        for w in [2, 3, 5]:
            if len(df) >= w:
                df[f'rolling_mean_{w}'] = target_series.rolling(window=w, min_periods=1).mean().shift(1).values
                df[f'rolling_std_{w}'] = target_series.rolling(window=w, min_periods=1).std().shift(1).fillna(0).values
                df[f'rolling_min_{w}'] = target_series.rolling(window=w, min_periods=1).min().shift(1).values
                df[f'rolling_max_{w}'] = target_series.rolling(window=w, min_periods=1).max().shift(1).values

        # 差分特征（shift(1) 确保不使用当前值计算差分）
        df['diff_1'] = target_series.diff().shift(1).fillna(0).values
        df['diff_2'] = target_series.diff(2).shift(1).fillna(0).values

        # 变化率（shift(1) 避免使用当前值）
        with np.errstate(divide='ignore', invalid='ignore'):
            rate = (target_series.diff().shift(1) / target_series.shift(1).replace(0, np.nan)).fillna(0)
            df['pct_change'] = rate.values

        # EWM（shift(1) 确保只用历史数据）
        for span in [3, 5]:
            df[f'ewm_{span}'] = target_series.ewm(span=span, adjust=False).mean().shift(1).values

        # 趋势强度（只用历史窗口 [i-3, i)，不含当前值）
        def trend_strength(s):
            if len(s) < 3:
                return 0.0
            x = np.arange(len(s))
            return np.polyfit(x, s, 1)[0]

        if len(df) >= 3:
            trend_vals = []
            for i in range(len(df)):
                if i < 3:
                    trend_vals.append(0.0)
                else:
                    window_data = target_series.iloc[max(0, i-3):i].values
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
                    win = y_arr[max(0, i - w):i]  # 只用历史窗口，不含当前值
                    row[f'rolling_mean_{w}'] = np.mean(win) if len(win) > 0 else 0
                    row[f'rolling_std_{w}'] = np.std(win) if len(win) > 0 else 0
                    row[f'rolling_min_{w}'] = np.min(win) if len(win) > 0 else 0
                    row[f'rolling_max_{w}'] = np.max(win) if len(win) > 0 else 0

            if 'diff_1' in feature_cols and i > 1:
                row['diff_1'] = y_arr[i - 1] - y_arr[i - 2]
            if 'diff_2' in feature_cols and i > 2:
                row['diff_2'] = y_arr[i - 1] - y_arr[i - 3]

            if 'pct_change' in feature_cols and i > 1 and y_arr[i - 2] != 0:
                row['pct_change'] = (y_arr[i - 1] - y_arr[i - 2]) / y_arr[i - 2]

            for span in [3, 5]:
                if f'ewm_{span}' in feature_cols and i > 0:
                    row[f'ewm_{span}'] = pd.Series(y_arr[:i]).ewm(span=span, adjust=False).mean().iloc[-1]

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
            df_raw_io: 原始 I/O DataFrame（来自 parse_io_traces）或已聚合时间序列 DataFrame
            target_col: 预测目标
            model_names: 模型列表
            n_lags: 滞后阶数（自动选择）
            predict_steps: 预测步数（自动选择）
        """
        # ---- 1. 选择最优时间窗口 ----
        if n_lags is None:
            n_lags = 5
        if predict_steps is None:
            predict_steps = 12
        if model_names is None:
            model_names = self.MEDIUM_DATA_MODELS

        # 检测数据类型：有 size/operation 列 = 原始 I/O trace，否则 = 已聚合时间序列
        IS_PRE_AGGREGATED = ('size' not in df_raw_io.columns
                             and 'operation' not in df_raw_io.columns
                             and target_col in df_raw_io.columns
                             and 'datetime' in df_raw_io.columns)

        if IS_PRE_AGGREGATED:
            # 已聚合数据：直接用 datetime 作为索引
            logger.info(f"  检测到已聚合时间序列数据，跳过窗口聚合")
            ts = df_raw_io.copy()
            if not isinstance(ts['datetime'].iloc[0], pd.Timestamp):
                ts['datetime'] = pd.to_datetime(ts['datetime'])
            window_sec = 'N/A'
            ts = ts.set_index('datetime')
            ts = ts[[target_col]].copy()
            ts = ts[ts[target_col].notna()]
        else:
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
        if IS_PRE_AGGREGATED:
            # ts 已经以 datetime 为索引，且仅包含目标列
            series = ts[target_col]
        else:
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
        """预测 vs 实际对比折线图：上半=全局概览，下半=预测区放大（预测点均匀拉开）"""
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
        # 每个 target 两行: [0]=全局概览, [1]=预测区放大
        fig, axes = plt.subplots(n_targets * 2, 1, figsize=(22, 8 * n_targets),
                                 gridspec_kw={'height_ratios': [3, 2] * n_targets})
        if n_targets == 1:
            axes = [axes[0], axes[1]]

        for ti, (target, result) in enumerate(valid_results.items()):
            ax_full = axes[ti * 2]       # 全局概览
            ax_zoom = axes[ti * 2 + 1]   # 预测区放大

            y_train = result['y_train']
            y_test = result['y_test']
            train_idx = result['train_index']
            test_idx = result['test_index']
            actual_test = y_test.values

            models_data = [(n, d) for n, d in result['models'].items() if 'predictions' in d]
            n_pred_pts = len(test_idx)

            # ========== 上半：全局概览 ==========
            ax_full.plot(train_idx, y_train.values, '-', color='#1a5276', linewidth=2.0,
                         alpha=0.9, label='历史负载', zorder=2)
            ax_full.plot(test_idx, actual_test, '-o', color='#2c3e50', linewidth=2.8,
                         markersize=10, markerfacecolor='white', markeredgewidth=2.2,
                         label='实际负载', zorder=6)

            for i, (model_name, data) in enumerate(models_data):
                style = model_styles.get(model_name, {})
                color = style.get('color', default_colors[i % 10])
                m = data['metrics']
                ax_full.plot(test_idx, data['predictions'], style.get('ls', '--'),
                             color=color, linewidth=2.4,
                             marker=style.get('marker', 'D'), markersize=8,
                             markeredgecolor=color, markerfacecolor='white',
                             markeredgewidth=1.5,
                             label=f'{model_name.replace("_"," ").title()} '
                                   f'(R2={m["R2"]:.3f},MAPE={m["MAPE"]:.1f}%)',
                             zorder=5)

            if n_pred_pts > 0:
                ax_full.axvline(x=test_idx[0], color='#e74c3c', linestyle='-', linewidth=2.0, alpha=0.7)
                ax_full.axvspan(test_idx[0], test_idx[-1], alpha=0.08, color='#f39c12', zorder=0)

            ylabel = target_labels.get(target, target)
            ax_full.set_title(f'{ylabel} — 全局概览 (预测{n_pred_pts}步)',
                              fontsize=15, fontweight='bold')
            ax_full.set_ylabel(ylabel, fontsize=11)
            ax_full.legend(fontsize=8, loc='upper left', framealpha=0.92, ncol=2)
            ax_full.grid(True, alpha=0.2, linestyle='--')
            ax_full.set_facecolor('#f8f9fa')
            ax_full.tick_params(labelbottom=True)
            # 时间轴格式化
            if isinstance(train_idx, pd.DatetimeIndex):
                ax_full.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax_full.tick_params(axis='x', rotation=30, labelsize=9)

            # ========== 下半：预测区放大（等间距 ordinal x） ==========
            # 取最后 50 个历史点 + 全部预测点，用 0,1,2,... 做 x 坐标均匀间隔
            zoom_hist_n = min(50, len(y_train))
            zoom_hist_vals = list(y_train.values[-zoom_hist_n:])

            # 时间标签
            if isinstance(train_idx, pd.DatetimeIndex):
                zoom_hist_labels = [t.strftime('%H:%M') for t in train_idx[-zoom_hist_n:]]
                zoom_test_labels = [t.strftime('%H:%M') for t in test_idx]
            else:
                zoom_hist_labels = [str(t)[-8:] for t in train_idx[-zoom_hist_n:]]
                zoom_test_labels = [str(t)[-8:] for t in test_idx]

            n_zoom = zoom_hist_n + n_pred_pts
            x_zoom = np.arange(n_zoom)

            # 历史尾巴
            x_hist = x_zoom[:zoom_hist_n]
            ax_zoom.plot(x_hist, zoom_hist_vals, '-', color='#1a5276', linewidth=2.0,
                         alpha=0.85, zorder=2)

            # 实际负载（预测区）
            x_pred = x_zoom[zoom_hist_n:]
            ax_zoom.plot(x_pred, actual_test, '-o', color='#2c3e50', linewidth=3.2,
                         markersize=12, markerfacecolor='white', markeredgewidth=2.8,
                         label='实际负载', zorder=6)

            # 各模型预测线
            for i, (model_name, data) in enumerate(models_data):
                style = model_styles.get(model_name, {})
                color = style.get('color', default_colors[i % 10])
                m = data['metrics']
                ax_zoom.plot(x_pred, data['predictions'],
                             style.get('ls', '--'), color=color, linewidth=3.0,
                             marker=style.get('marker', 'D'), markersize=11,
                             markeredgecolor=color, markerfacecolor='white',
                             markeredgewidth=2.2,
                             label=f'{model_name.replace("_"," ").title()} '
                                   f'(R2={m["R2"]:.3f},MAPE={m["MAPE"]:.1f}%)',
                             zorder=5)

            # 分界线
            if zoom_hist_n > 0:
                ax_zoom.axvline(x=zoom_hist_n - 0.5, color='#e74c3c', linestyle='-',
                                linewidth=2.5, alpha=0.8)
                ax_zoom.axvspan(zoom_hist_n - 0.5, n_zoom - 0.5, alpha=0.10,
                                color='#f39c12', zorder=0)

            # 最佳模型偏差填充
            if models_data:
                best = min(models_data, key=lambda x: x[1]['metrics']['RMSE'])
                ax_zoom.fill_between(x_pred, actual_test, best[1]['predictions'],
                                     alpha=0.15, color='#27ae60', zorder=1)

            # X 轴刻度标签
            all_labels = zoom_hist_labels + zoom_test_labels
            # 历史段抽稀，预测段全标
            tick_positions = []
            tick_labels = []
            if zoom_hist_n > 0:
                hist_step = max(1, zoom_hist_n // 6)
                for p in range(0, zoom_hist_n, hist_step):
                    tick_positions.append(p)
                    tick_labels.append(all_labels[p])
            for p in range(n_pred_pts):
                tick_positions.append(zoom_hist_n + p)
                tick_labels.append(all_labels[zoom_hist_n + p])

            ax_zoom.set_xticks(tick_positions)
            ax_zoom.set_xticklabels(tick_labels, rotation=40, ha='right', fontsize=9)

            # 数值标注（预测段每个点都标）
            for j in range(n_pred_pts):
                xv, av = x_pred[j], actual_test[j]
                ax_zoom.annotate(f'{av:.1f}', (xv, av),
                                 textcoords="offset points", xytext=(0, 16),
                                 ha='center', fontsize=10, fontweight='bold',
                                 color='#2c3e50',
                                 bbox=dict(boxstyle='round,pad=0.2', facecolor='white',
                                           edgecolor='#2c3e50', alpha=0.9))
                for k, (model_name, data) in enumerate(models_data):
                    pv = data['predictions'][j]
                    style = model_styles.get(model_name, {})
                    color = style.get('color', default_colors[k % 10])
                    ox = (k - len(models_data) / 2 + 0.5) * 35
                    ax_zoom.annotate(f'{pv:.1f}', (xv, pv),
                                     textcoords="offset points",
                                     xytext=(ox, -20 - (k % 2) * 16),
                                     ha='center', fontsize=8, color=color,
                                     fontweight='bold')

            ax_zoom.set_title(f'预测区放大 — 等间距展示 {n_pred_pts} 个预测步',
                              fontsize=14, fontweight='bold', color='#c0392b')
            ax_zoom.set_xlabel('时间（预测区展开）', fontsize=11)
            ax_zoom.set_ylabel(ylabel, fontsize=11)
            ax_zoom.legend(fontsize=8.5, loc='upper left', framealpha=0.93, ncol=2)
            ax_zoom.grid(True, alpha=0.25, linestyle='--')
            ax_zoom.set_facecolor('#fffef5')

            # Y 轴范围（基于 zoom 窗口数据）
            all_zv = list(zoom_hist_vals) + list(actual_test)
            for _, d in models_data:
                all_zv += list(d['predictions'])
            z_lo, z_hi = min(all_zv), max(all_zv)
            zm = (z_hi - z_lo) * 0.18 if z_hi != z_lo else 1
            ax_zoom.set_ylim(z_lo - zm, z_hi + zm * 2.5)

            # 全局 Y 轴也同步
            all_v = list(y_train.values) + list(actual_test)
            for _, d in models_data:
                all_v += list(d['predictions'])
            y_lo, y_hi = min(all_v), max(all_v)
            m = (y_hi - y_lo) * 0.15 if y_hi != y_lo else 1
            ax_full.set_ylim(y_lo - m, y_hi + m * 2.8)

        plt.tight_layout(pad=3.0)
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
            bp = axes[0].boxplot(residuals_all, patch_artist=True, showmeans=True)
            colors = plt.cm.Pastel1(np.linspace(0, 1, len(residuals_all)))
            for p, c in zip(bp['boxes'], colors):
                p.set_facecolor(c)
            axes[0].set_xticklabels(labels, fontsize=7, rotation=30, ha='right')
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
        """综合仪表盘（含预测区放大面板）"""
        valid_results = {k: v for k, v in all_results.items() if 'models' in v}
        if not valid_results:
            return

        target_labels = {
            'iops': '总 IOPS', 'iops_read': '读 IOPS', 'iops_write': '写 IOPS',
            'throughput_kb': '吞吐量 (KB/s)',
        }

        fig = plt.figure(figsize=(24, 20))
        fig.suptitle('I/O 负载预测分析仪表盘 v2', fontsize=20, fontweight='bold', y=0.99)

        gs = fig.add_gridspec(5, 3, hspace=0.45, wspace=0.35,
                              height_ratios=[1.1, 0.9, 0.9, 0.9, 0.7])

        model_styles_dash = {
            'linear': '#e74c3c', 'ridge': '#3498db', 'lasso': '#9b59b6',
            'random_forest': '#2ecc71', 'gradient_boosting': '#f39c12',
            'exp_smoothing': '#1abc9c', 'ar1': '#e91e63', 'trend': '#00bcd4',
        }

        target = list(valid_results.keys())[0]
        result = valid_results[target]
        y_train, y_test = result['y_train'], result['y_test']
        train_idx, test_idx = result['train_index'], result['test_index']
        actual = y_test.values
        models_data_dash = [(mn, d) for mn, d in result['models'].items()
                            if 'predictions' in d]

        # ---- Row 0：全局概览 ----
        ax_main = fig.add_subplot(gs[0, :])
        ax_main.plot(train_idx, y_train.values, '-', color='#1a5276', linewidth=2.0,
                     alpha=0.9, label='历史负载', zorder=2)
        ax_main.plot(test_idx, actual, '-o', color='#2c3e50', linewidth=2.8,
                     markersize=10, markerfacecolor='white', markeredgewidth=2.2,
                     label='实际负载', zorder=6)

        for mn, data in models_data_dash:
            color = model_styles_dash.get(mn, '#999')
            m = data['metrics']
            ax_main.plot(test_idx, data['predictions'], '--', color=color, linewidth=2.4,
                         marker='D', markersize=8, markeredgecolor=color,
                         markerfacecolor='white', markeredgewidth=1.5,
                         label=f"{mn.replace('_',' ').title()} "
                               f"(Acc={m['Accuracy']:.0f}%,R2={m['R2']:.3f})", zorder=5)

        ax_main.axvline(x=test_idx[0], color='#e74c3c', linestyle='-', linewidth=2.0, alpha=0.7)
        ax_main.axvspan(test_idx[0], test_idx[-1], alpha=0.08, color='#f39c12', zorder=0)

        best_mk = min((k for k in result['models'] if 'metrics' in result['models'][k]),
                      key=lambda x: result['models'][x]['metrics']['RMSE'], default=None)
        if best_mk:
            ax_main.fill_between(test_idx, actual,
                                 result['models'][best_mk]['predictions'],
                                 alpha=0.12, color='#27ae60')

        ylabel = target_labels.get(target, target)
        ax_main.set_title(f'全局概览 — {ylabel} (预测{len(test_idx)}步)',
                         fontsize=14, fontweight='bold')
        ax_main.set_ylabel(ylabel, fontsize=11)
        ax_main.legend(fontsize=7.5, loc='upper left', ncol=3, framealpha=0.92)
        ax_main.grid(True, alpha=0.2, linestyle='--')
        ax_main.set_facecolor('#f8f9fa')
        ax_main.tick_params(labelbottom=True)
        if isinstance(train_idx, pd.DatetimeIndex):
            ax_main.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax_main.tick_params(axis='x', rotation=30, labelsize=9)

        all_v = list(y_train.values) + list(actual)
        for md in result['models'].values():
            if 'predictions' in md:
                all_v += list(md['predictions'])
        y_lo, y_hi = min(all_v), max(all_v)
        ym = (y_hi - y_lo) * 0.15 if y_hi != y_lo else 1
        ax_main.set_ylim(y_lo - ym, y_hi + ym * 2.8)

        # ---- Row 1：预测区放大（等间距 ordinal x） ----
        ax_zoom = fig.add_subplot(gs[1, :])
        zoom_hist_n = min(50, len(y_train))
        zoom_hist_vals = list(y_train.values[-zoom_hist_n:])
        n_zoom = zoom_hist_n + len(test_idx)
        x_zoom = np.arange(n_zoom)
        x_hist_z = x_zoom[:zoom_hist_n]
        x_pred_z = x_zoom[zoom_hist_n:]

        ax_zoom.plot(x_hist_z, zoom_hist_vals, '-', color='#1a5276', linewidth=2.0,
                     alpha=0.85, zorder=2)
        ax_zoom.plot(x_pred_z, actual, '-o', color='#2c3e50', linewidth=3.2,
                     markersize=12, markerfacecolor='white', markeredgewidth=2.8,
                     label='实际负载', zorder=6)

        for mn, data in models_data_dash:
            color = model_styles_dash.get(mn, '#999')
            m = data['metrics']
            ax_zoom.plot(x_pred_z, data['predictions'], '--', color=color, linewidth=3.0,
                         marker='D', markersize=11, markeredgecolor=color,
                         markerfacecolor='white', markeredgewidth=2.2,
                         label=f"{mn.replace('_',' ').title()} "
                               f"(Acc={m['Accuracy']:.0f}%,R2={m['R2']:.3f})", zorder=5)

        if zoom_hist_n > 0:
            ax_zoom.axvline(x=zoom_hist_n - 0.5, color='#e74c3c', linestyle='-',
                            linewidth=2.5, alpha=0.8)
            ax_zoom.axvspan(zoom_hist_n - 0.5, n_zoom - 0.5, alpha=0.10,
                            color='#f39c12', zorder=0)

        if best_mk:
            ax_zoom.fill_between(x_pred_z, actual,
                                 result['models'][best_mk]['predictions'],
                                 alpha=0.15, color='#27ae60', zorder=1)

        # X 轴标签
        all_labels_z = []
        if isinstance(train_idx, pd.DatetimeIndex):
            all_labels_z += [t.strftime('%H:%M') for t in train_idx[-zoom_hist_n:]]
            all_labels_z += [t.strftime('%H:%M') for t in test_idx]
        else:
            all_labels_z += [str(t)[-8:] for t in train_idx[-zoom_hist_n:]]
            all_labels_z += [str(t)[-8:] for t in test_idx]
        tick_p, tick_l = [], []
        if zoom_hist_n > 0:
            for p in range(0, zoom_hist_n, max(1, zoom_hist_n // 6)):
                tick_p.append(p); tick_l.append(all_labels_z[p])
        for p in range(len(test_idx)):
            tick_p.append(zoom_hist_n + p)
            tick_l.append(all_labels_z[zoom_hist_n + p])
        ax_zoom.set_xticks(tick_p)
        ax_zoom.set_xticklabels(tick_l, rotation=40, ha='right', fontsize=9)

        # 预测点数值标注（全标）
        for j in range(len(test_idx)):
            xv, av = x_pred_z[j], actual[j]
            ax_zoom.annotate(f'{av:.1f}', (xv, av), textcoords="offset points",
                             xytext=(0, 16), ha='center', fontsize=10, fontweight='bold',
                             color='#2c3e50',
                             bbox=dict(boxstyle='round,pad=0.2', facecolor='white',
                                       edgecolor='#2c3e50', alpha=0.9))

        ax_zoom.set_title(f'预测区放大 — {len(test_idx)} 步等间距展开',
                         fontsize=13, fontweight='bold', color='#c0392b')
        ax_zoom.set_xlabel('时间（预测区展开）', fontsize=11)
        ax_zoom.set_ylabel(ylabel, fontsize=11)
        ax_zoom.legend(fontsize=8, loc='upper left', ncol=3, framealpha=0.93)
        ax_zoom.grid(True, alpha=0.25, linestyle='--')
        ax_zoom.set_facecolor('#fffef5')

        zall = zoom_hist_vals + list(actual)
        for _, d in models_data_dash:
            zall += list(d['predictions'])
        zl, zh = min(zall), max(zall)
        zm2 = (zh - zl) * 0.18 if zh != zl else 1
        ax_zoom.set_ylim(zl - zm2, zh + zm2 * 2.5)

        # ---- Row 2：指标表 ----
        ax_table = fig.add_subplot(gs[2, :])
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

        # ---- Row 3：MAPE / R2 / Acc ----
        ax_mape = fig.add_subplot(gs[3, 0])
        mape_data, mape_labels = [], []
        for tgt, res in valid_results.items():
            for mn, md in res['models'].items():
                if 'metrics' in md:
                    mape_data.append(md['metrics']['MAPE'])
                    mape_labels.append(f"{mn[:10]}\n({tgt[:8]})")
        if mape_data:
            colors_m = ['#27ae60' if v < 10 else '#f39c12' if v < 30 else '#e74c3c' for v in mape_data]
            ax_mape.barh(range(len(mape_data)), mape_data, color=colors_m)
            ax_mape.set_yticks(range(len(mape_labels)))
            ax_mape.set_yticklabels(mape_labels, fontsize=8)
            ax_mape.set_title('MAPE (%)', fontsize=12, fontweight='bold')
            ax_mape.grid(True, alpha=0.3, axis='x')

        ax_r2 = fig.add_subplot(gs[3, 1])
        r2_data, r2_labels = [], []
        for tgt, res in valid_results.items():
            for mn, md in res['models'].items():
                if 'metrics' in md:
                    r2_data.append(md['metrics']['R2'])
                    r2_labels.append(f"{mn[:10]}\n({tgt[:8]})")
        if r2_data:
            ax_r2.bar(range(len(r2_data)), r2_data,
                      color=plt.cm.RdYlGn(np.clip(r2_data, 0, 1)))
            ax_r2.set_xticks(range(len(r2_labels)))
            ax_r2.set_xticklabels(r2_labels, fontsize=7, rotation=30, ha='right')
            ax_r2.axhline(y=0.8, color='green', linestyle='--', alpha=0.5, label='>=0.8 优秀')
            ax_r2.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            ax_r2.set_title('R2 决定系数', fontsize=12, fontweight='bold')
            ax_r2.set_ylim(min(0, min(r2_data) - 0.1), 1.05)
            ax_r2.legend(fontsize=8)
            ax_r2.grid(True, alpha=0.3, axis='y')

        ax_acc = fig.add_subplot(gs[3, 2])
        acc_data, acc_labels = [], []
        for tgt, res in valid_results.items():
            for mn, md in res['models'].items():
                if 'metrics' in md:
                    acc_data.append(md['metrics']['Accuracy'])
                    acc_labels.append(f"{mn[:10]}\n({tgt[:8]})")
        if acc_data:
            colors_a = ['#27ae60' if v >= 90 else '#f39c12' if v >= 70 else '#e74c3c'
                        for v in acc_data]
            ax_acc.bar(range(len(acc_data)), acc_data, color=colors_a)
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
