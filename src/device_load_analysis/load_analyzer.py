"""
设备负载分析模块
分析块设备 I/O 负载，为预测和可视化提供数据基础

原始数据字段说明（io_traces.csv，无表头）:
  - device_id: 设备编号 (0-999)
  - operation: I/O 操作类型 (R=读, W=写)
  - offset: 块偏移地址 (字节)
  - size: 请求大小 (字节)
  - timestamp: 微秒级时间戳

device_size.csv 字段:
  - device_id: 设备编号
  - disk_size: 磁盘大小 (字节)
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class DeviceLoadAnalyzer:
    """设备负载分析器"""

    IO_TRACE_COLUMNS = ['device_id', 'operation', 'offset', 'size', 'timestamp']
    DEVICE_SIZE_COLUMNS = ['device_id', 'disk_size']

    def __init__(self, config=None):
        """
        Args:
            config: 配置字典，支持:
                - time_window: 时间聚合窗口(秒)，默认60
                - top_n_devices: 分析前N个设备，默认None(全部)
                - hotspot_threshold: 热点块访问次数阈值，默认100
        """
        self.config = config or {}
        self.time_window = self.config.get('time_window', 60)
        self.top_n_devices = self.config.get('top_n_devices', None)
        self.hotspot_threshold = self.config.get('hotspot_threshold', 100)
        self.analysis_results = {}
        logger.info(f"设备负载分析器初始化完成，时间窗口: {self.time_window}s")

    # ================================================================
    # 数据解析
    # ================================================================

    def parse_io_traces(self, df):
        """解析原始 I/O 轨迹数据，标准化列名和类型"""
        df = df.copy()
        logger.info(f"parse_io_traces 输入: shape={df.shape}, 列名={list(df.columns)[:10]}, 列类型={[type(c).__name__ for c in df.columns[:5]]}")

        mapped = False

        # 兼容整数列名和字符串数字列名 (如 '0','1','2','3','4')
        if all(isinstance(c, (int, str)) for c in df.columns):
            str_cols = [str(c).strip() for c in df.columns]
            if str_cols[:5] == ['0', '1', '2', '3', '4']:
                df = df.iloc[:, :5]
                df.columns = self.IO_TRACE_COLUMNS
                mapped = True
            elif len(df.columns) >= 5 and str_cols[:5] == ['0', '1', '2', '3', '4']:
                df = df.iloc[:, :5]
                df.columns = self.IO_TRACE_COLUMNS
                mapped = True
            elif len(df.columns) == 5 and str_cols != self.IO_TRACE_COLUMNS:
                # 列名不是标准名但数量匹配，尝试检测 R/W 列
                for col in df.columns:
                    unique_vals = df[col].dropna().astype(str).unique()
                    if len(unique_vals) <= 2 and any(v.upper() in ['R', 'W'] for v in unique_vals):
                        df = df.iloc[:, :5]
                        df.columns = self.IO_TRACE_COLUMNS
                        mapped = True
                        break

        # 兜底：如果列名仍未映射但已知列名模式不匹配，尝试按位置取前5列
        if not mapped and len(df.columns) > 5:
            logger.info(f"列名未自动映射，尝试按位置取前5列")
            df = df.iloc[:, :5]
            # 检测 R/W 列位置
            for i in range(5):
                unique_vals = df.iloc[:, i].dropna().astype(str).unique()
                if len(unique_vals) <= 2 and any(v.upper() in ['R', 'W'] for v in unique_vals):
                    df.columns = self.IO_TRACE_COLUMNS
                    mapped = True
                    break

        if not mapped and list(df.columns) != self.IO_TRACE_COLUMNS:
            logger.warning(f"列名映射失败，当前列名: {list(df.columns)}")

        df['device_id'] = pd.to_numeric(df['device_id'], errors='coerce').astype('Int64')
        df['operation'] = df['operation'].astype(str).str.upper().str.strip()
        df['offset'] = pd.to_numeric(df['offset'], errors='coerce')
        df['size'] = pd.to_numeric(df['size'], errors='coerce')
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')

        initial_count = len(df)
        df = df.dropna(subset=['device_id', 'operation', 'offset', 'size', 'timestamp'])
        df = df[df['operation'].isin(['R', 'W'])]
        if len(df) < initial_count:
            logger.info(f"过滤无效行: {initial_count} -> {len(df)}")

        df['datetime'] = pd.to_datetime(df['timestamp'], unit='us')
        return df

    def parse_device_sizes(self, df):
        """解析设备大小数据"""
        df = df.copy()
        if all(isinstance(c, int) for c in df.columns):
            if len(df.columns) == 2:
                df.columns = self.DEVICE_SIZE_COLUMNS
        df['device_id'] = pd.to_numeric(df['device_id'], errors='coerce').astype('Int64')
        df['disk_size'] = pd.to_numeric(df['disk_size'], errors='coerce')
        df = df.dropna()
        return df.set_index('device_id')['disk_size']

    # ================================================================
    # 核心指标聚合
    # ================================================================

    def aggregate_load(self, df):
        """按时间窗口 + 设备聚合负载指标（IOPS、吞吐量、读写比等）"""
        logger.info("开始按时间窗口聚合负载指标...")

        window_sec = self.time_window
        df = df.copy()
        df['time_window'] = df['datetime'].dt.floor(f'{window_sec}s')
        df['is_read'] = (df['operation'] == 'R').astype(int)
        df['is_write'] = (df['operation'] == 'W').astype(int)
        df['size_kb'] = df['size'] / 1024.0

        grouped = df.groupby(['time_window', 'device_id'])
        agg_df = grouped.agg(
            read_count=('is_read', 'sum'),
            write_count=('is_write', 'sum'),
            total_count=('operation', 'count'),
            read_bytes=('is_read', lambda x: (x * df.loc[x.index, 'size']).sum()),
            write_bytes=('is_write', lambda x: (x * df.loc[x.index, 'size']).sum()),
            avg_request_size=('size_kb', 'mean'),
        ).reset_index()

        agg_df['iops_read'] = agg_df['read_count'] / window_sec
        agg_df['iops_write'] = agg_df['write_count'] / window_sec
        agg_df['iops_total'] = agg_df['total_count'] / window_sec
        agg_df['throughput_read_kb'] = agg_df['read_bytes'] / window_sec / 1024.0
        agg_df['throughput_write_kb'] = agg_df['write_bytes'] / window_sec / 1024.0
        agg_df['throughput_total_kb'] = agg_df['throughput_read_kb'] + agg_df['throughput_write_kb']
        agg_df['read_ratio'] = agg_df['read_count'] / agg_df['total_count'].replace(0, np.nan)

        logger.info(f"负载聚合完成: {len(agg_df)} 条记录")
        self.analysis_results['aggregated_load'] = agg_df
        return agg_df

    # ================================================================
    # 设备画像
    # ================================================================

    def analyze_device_profiles(self, df):
        """设备负载画像分析（请求数、IOPS、峰值、活跃时段等）"""
        logger.info("开始设备画像分析...")

        df = df.copy()
        df['time_window'] = df['datetime'].dt.floor(f'{self.time_window}s')
        df['size_kb'] = df['size'] / 1024.0

        profiles = df.groupby('device_id').agg(
            total_requests=('operation', 'count'),
            total_bytes=('size', 'sum'),
            avg_request_size_kb=('size_kb', 'mean'),
            std_request_size_kb=('size_kb', 'std'),
            first_active=('datetime', 'min'),
            last_active=('datetime', 'max'),
            active_windows=('time_window', 'nunique'),
        ).reset_index()

        # 读写请求计数
        read_counts = df[df['operation'] == 'R'].groupby('device_id').size().rename('read_requests')
        write_counts = df[df['operation'] == 'W'].groupby('device_id').size().rename('write_requests')
        profiles = profiles.merge(read_counts, left_on='device_id', right_index=True, how='left')
        profiles = profiles.merge(write_counts, left_on='device_id', right_index=True, how='left')
        profiles['read_requests'] = profiles['read_requests'].fillna(0).astype(int)
        profiles['write_requests'] = profiles['write_requests'].fillna(0).astype(int)

        # 衍生指标
        profiles['read_bytes'] = profiles['total_bytes'] * profiles['read_requests'] / profiles['total_requests'].replace(0, np.nan)
        profiles['write_bytes'] = profiles['total_bytes'] - profiles['read_bytes']
        profiles['read_ratio'] = profiles['read_requests'] / profiles['total_requests'].replace(0, np.nan)
        profiles['avg_iops'] = profiles['total_requests'] / (profiles['active_windows'] * self.time_window).replace(0, np.nan)
        profiles['active_span_hours'] = (profiles['last_active'] - profiles['first_active']).dt.total_seconds() / 3600.0

        # 峰值小时
        df['hour'] = df['datetime'].dt.hour
        peak = df.groupby('device_id')['hour'].apply(
            lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else np.nan
        ).rename('peak_hour')
        profiles = profiles.merge(peak, left_on='device_id', right_index=True, how='left')

        # 峰值IOPS
        peak_iops = df.groupby(['device_id', 'time_window']).size().groupby('device_id').max() / self.time_window
        peak_iops.name = 'peak_iops'
        profiles = profiles.merge(peak_iops, left_on='device_id', right_index=True, how='left')

        profiles = profiles.sort_values('total_requests', ascending=False).reset_index(drop=True)

        logger.info(f"设备画像分析完成: {len(profiles)} 个设备")
        self.analysis_results['device_profiles'] = profiles
        return profiles

    def identify_hot_devices(self, profiles, top_n=None):
        """识别高负载设备"""
        top_n = top_n or self.top_n_devices or len(profiles)
        hot = profiles.nlargest(min(top_n, len(profiles)), 'total_requests')
        logger.info(f"识别出 {len(hot)} 个高负载设备")
        self.analysis_results['hot_devices'] = hot
        return hot

    # ================================================================
    # 空间访问分析
    # ================================================================

    def analyze_hotspots(self, df, top_n=20):
        """热点块分析：将磁盘按1MB分块，统计访问频率"""
        logger.info("开始热点块分析...")

        block_size = 1024 * 1024
        df = df.copy()
        df['block_id'] = (df['offset'] // block_size).astype('Int64')

        block_stats = df.groupby(['device_id', 'block_id']).agg(
            access_count=('operation', 'count'),
            read_count=('operation', lambda x: (x == 'R').sum()),
            write_count=('operation', lambda x: (x == 'W').sum()),
            total_bytes=('size', 'sum'),
            avg_request_size=('size', 'mean'),
        ).reset_index()

        hotspots = block_stats[block_stats['access_count'] >= self.hotspot_threshold]
        hotspots = hotspots.sort_values('access_count', ascending=False)
        if top_n and len(hotspots) > top_n:
            hotspots = hotspots.head(top_n)
        hotspots['block_start_offset_mb'] = hotspots['block_id'] * block_size / (1024 * 1024)

        logger.info(f"热点块分析完成: 共{len(block_stats)}个块，热点{len(hotspots)}个")
        self.analysis_results['hotspots'] = hotspots
        return hotspots

    # ================================================================
    # 访问模式分析
    # ================================================================

    def analyze_access_patterns(self, df):
        """分析I/O访问的时序模式（顺序度、读写比、时间聚集性、小时分布）"""
        logger.info("开始访问模式分析...")

        results = {}

        # 全局读写比
        read_ratio = (df['operation'] == 'R').mean()
        results['global_read_ratio'] = read_ratio
        results['global_write_ratio'] = 1 - read_ratio

        # 请求大小
        results['request_size_stats'] = df['size'].describe().to_dict()
        size_mode = df['size'].mode()
        results['dominant_request_size'] = int(size_mode.iloc[0]) if len(size_mode) > 0 else None

        # 顺序度（按设备分组）
        sequential_scores = []
        df_sorted = df.sort_values(['device_id', 'timestamp']).reset_index(drop=True)
        for _, group in df_sorted.groupby('device_id'):
            if len(group) < 2:
                continue
            offsets = group['offset'].values
            seq_count = np.sum(offsets[1:] >= offsets[:-1])
            sequential_scores.append(seq_count / (len(offsets) - 1))

        if sequential_scores:
            results['sequential_access_ratio'] = np.mean(sequential_scores)
            results['sequential_access_std'] = np.std(sequential_scores)

        # 时间聚集性
        df = df.copy()
        df['time_window'] = df['datetime'].dt.floor(f'{self.time_window}s')
        total_windows = (df['time_window'].max() - df['time_window'].min()).total_seconds() / self.time_window
        active_windows = df['time_window'].nunique()
        results['active_window_ratio'] = active_windows / max(total_windows, 1) if total_windows > 0 else 0
        results['total_time_windows'] = int(total_windows)
        results['active_time_windows'] = active_windows

        # 小时分布
        df['hour'] = df['datetime'].dt.hour
        hourly_dist = df.groupby('hour').size()
        results['hourly_distribution'] = hourly_dist.to_dict()
        results['peak_hour'] = int(hourly_dist.idxmax())
        results['off_peak_hour'] = int(hourly_dist.idxmin())

        # 按分钟粒度分布（适合测试阶段短时间跨度数据）
        df['minute_key'] = df['datetime'].dt.strftime('%H:%M')
        minute_dist = df.groupby('minute_key').size()
        results['minute_distribution'] = minute_dist.to_dict()

        # 按秒粒度分布（适合短时间跨度数据，用于折线图）
        df['second_key'] = df['datetime'].dt.strftime('%H:%M:%S')
        second_dist = df.groupby('second_key').size()
        results['second_distribution'] = second_dist.to_dict()

        logger.info(f"访问模式分析完成: 顺序度={results.get('sequential_access_ratio', 0):.2%}")
        self.analysis_results['access_patterns'] = results
        return results

    # ================================================================
    # 时间序列负载
    # ================================================================

    def compute_time_series_load(self, df, level='global'):
        """
        计算时间序列负载数据（用于预测和可视化）

        Args:
            df: 已解析的I/O轨迹DataFrame
            level: 'global' | 'device'
        """
        logger.info(f"计算时间序列负载 (level={level})...")

        window_str = f'{self.time_window}s'
        df = df.copy()
        df['time_window'] = df['datetime'].dt.floor(window_str)
        df['size_kb'] = df['size'] / 1024.0

        if level == 'global':
            ts = df.groupby('time_window').agg(
                iops=('operation', 'count'),
                read_ops=('operation', lambda x: (x == 'R').sum()),
                write_ops=('operation', lambda x: (x == 'W').sum()),
                throughput_kb=('size_kb', 'sum'),
                avg_request_size_kb=('size_kb', 'mean'),
                distinct_devices=('device_id', 'nunique'),
            ).reset_index()
        elif level == 'device':
            ts = df.groupby(['device_id', 'time_window']).agg(
                iops=('operation', 'count'),
                read_ops=('operation', lambda x: (x == 'R').sum()),
                write_ops=('operation', lambda x: (x == 'W').sum()),
                throughput_kb=('size_kb', 'sum'),
                avg_request_size_kb=('size_kb', 'mean'),
            ).reset_index()
        else:
            raise ValueError(f"不支持的聚合级别: {level}")

        ts['iops'] = ts['iops'] / self.time_window
        ts['iops_read'] = ts['read_ops'] / self.time_window
        ts['iops_write'] = ts['write_ops'] / self.time_window
        ts['throughput_kb'] = ts['throughput_kb'] / self.time_window
        ts['read_ratio'] = ts['read_ops'] / (ts['read_ops'] + ts['write_ops']).replace(0, np.nan)
        ts = ts.rename(columns={'time_window': 'datetime'})

        logger.info(f"时间序列负载计算完成: {len(ts)} 条记录")
        self.analysis_results[f'time_series_{level}'] = ts
        return ts

    # ================================================================
    # 完整分析流程
    # ================================================================

    def run_full_analysis(self, df, save_dir=None):
        """
        运行完整的设备负载分析

        Args:
            df: 原始I/O轨迹DataFrame
            save_dir: 结果保存目录（同时生成可视化图表）

        Returns:
            分析结果字典
        """
        from .load_reporter import LoadReporter

        logger.info("=" * 60)
        logger.info("开始完整设备负载分析")
        logger.info("=" * 60)

        df = self.parse_io_traces(df)
        logger.info(f"解析后数据: {len(df)} 条记录, {df['device_id'].nunique()} 个设备")
        logger.info(f"时间范围: {df['datetime'].min()} ~ {df['datetime'].max()}")

        # 1. 时间序列负载聚合
        self.compute_time_series_load(df, level='global')
        self.compute_time_series_load(df, level='device')

        # 2. 设备画像
        profiles = self.analyze_device_profiles(df)

        # 3. 高负载设备
        self.identify_hot_devices(profiles)

        # 4. 热点块
        self.analyze_hotspots(df)

        # 5. 访问模式
        patterns = self.analyze_access_patterns(df)

        # 报告
        reporter = LoadReporter(save_dir=save_dir)
        ts_global = self.analysis_results.get('time_series_global', pd.DataFrame())

        reporter.print_summary(profiles, patterns, ts_global)

        if save_dir:
            reporter.save_results(self.analysis_results, save_dir)
            reporter.visualize_load(self.analysis_results, save_dir)
            reporter.generate_html_report(self.analysis_results, save_dir)

        logger.info("完整设备负载分析结束")
        return self.analysis_results
