"""
流式Tar.gz处理器（支持断点续传）
================================

处理大规模 tar.gz 文件的流式方案：
1. HTTP Range 下载 + 边下边解压边处理
2. 只保留聚合分析结果，丢弃原始数据
3. checkpoints 支持中断后从上一次位置继续
4. 增量聚合：device profiles、时间序列、热点、访问模式

适用于 20-50GB 级别的阿里云 OSS trace 数据。
"""

import os
import io
import gzip
import json
import time
import tarfile
import threading
import logging
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

# ================================================================
# 数据常量
# ================================================================
IO_TRACE_COLUMNS = ['device_id', 'operation', 'offset', 'size', 'timestamp']
BLOCK_SIZE_MB = 1024 * 1024  # 1MB 磁盘块大小


class CheckpointManager:
    """checkpoint 管理器：保存/加载处理进度与聚合结果"""

    def __init__(self, work_dir):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.work_dir / 'checkpoint.json'

    def save(self, state: dict):
        """序列化并保存 checkpoint"""
        state['_saved_at'] = datetime.now().isoformat()
        state['_version'] = 2
        serialized = self._to_serializable(state)
        tmp = str(self.checkpoint_file) + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(serialized, f, ensure_ascii=False)
        os.replace(tmp, str(self.checkpoint_file))
        logger.info(f"Checkpoint 已保存: {len(json.dumps(serialized))} 字节")

    def load(self) -> dict:
        """加载 checkpoint"""
        if not self.checkpoint_file.exists():
            logger.info("未发现 checkpoint，从头开始")
            return None
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            logger.info(f"Checkpoint 已加载 (版本 {state.get('_version', 1)}), "
                        f"已处理 {state.get('total_bytes_downloaded', 0) / (1024**3):.2f} GB, "
                        f"已处理 {state.get('total_rows_processed', 0):,} 行")
            return state
        except Exception as e:
            logger.warning(f"加载 checkpoint 失败: {e}，将从头开始")
            return None

    def exists(self) -> bool:
        return self.checkpoint_file.exists()

    def clear(self):
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

    @staticmethod
    def _to_serializable(obj):
        """递归转换 numpy 类型为原生 Python 类型"""
        if isinstance(obj, dict):
            return {str(k): CheckpointManager._to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [CheckpointManager._to_serializable(v) for v in obj]
        elif isinstance(obj, set):
            return sorted(obj)
        elif isinstance(obj, (np.integer,)):
            return int(obj)
        elif isinstance(obj, (np.floating,)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (pd.Timestamp,)):
            return obj.isoformat()
        else:
            return obj


# ================================================================
# 增量聚合器
# ================================================================

class IncrementalAggregator:
    """
    增量聚合器：逐块处理 I/O 轨迹数据，累积统计指标。
    支持从 checkpoint 恢复状态。

    优化策略：
    - sample_ratio: 采样率，跳过部分行以加速处理（1.0=全量）
    - max_rows: 达到目标行数后自动停止
    - top_device_ts: 只为 Top N 设备保留详细时间序列
    - 跳过大文件场景下的秒级分布（second_distribution）
    - 定期清理低于阈值的冷门热点块以节省内存

    累积的指标：
    - 设备维度：请求量、字节量、读写比、活跃窗口、峰值小时、峰值 IOPS
    - 时间序列：按时间窗口聚合 IOPS、吞吐量
    - 热点块：按 (device, block) 统计访问频次
    - 全局访问模式：顺序度、请求大小分布、读写比、小时分布
    """

    def __init__(self, config=None):
        self.config = config or {}
        self.time_window_sec = self.config.get('time_window', 60)
        self.hotspot_threshold = self.config.get('hotspot_threshold', 100)
        self.max_rows = self.config.get('max_rows', 0)
        self.sample_ratio = self.config.get('sample_ratio', 1.0)
        self.top_device_ts = self.config.get('top_device_ts', 50)
        self.skip_second_dist = self.config.get('skip_second_dist', False)
        self._block_cleanup_counter = 0
        self.reset()

    def reset(self):
        """重置所有聚合状态"""
        # ---- 设备级别聚合 ----
        self.dev_stats = defaultdict(lambda: {
            'total_count': 0, 'read_count': 0, 'write_count': 0,
            'total_bytes': 0, 'sum_size': 0.0, 'sum_size_sq': 0.0,
            'first_active_us': None, 'last_active_us': None,
            'window_counts': defaultdict(int),  # time_window_key -> count
            'hour_counts': defaultdict(int),    # hour (int) -> count
            'last_offset': None,                # 用于顺序度计算
            'sequential_pairs': 0,              # 顺序访问对数
            'total_pairs': 0,                   # 总相邻访问对数
        })
        # ---- 时间序列聚合 ----
        self.ts_global = defaultdict(lambda: {
            'total_count': 0, 'read_count': 0, 'write_count': 0,
            'total_size_kb': 0.0, 'sum_size_kb': 0.0,
            'distinct_devices': set(),   # 精确去重，上限约3000/窗口
        })
        self.ts_device = defaultdict(lambda: defaultdict(lambda: {
            'total_count': 0, 'read_count': 0, 'write_count': 0,
            'total_size_kb': 0.0, 'sum_size_kb': 0.0,
        }))
        # ---- 热点块聚合 ----
        self.block_stats = defaultdict(lambda: {
            'access_count': 0, 'read_count': 0, 'write_count': 0,
            'total_bytes': 0, 'sum_size': 0.0,
        })
        # ---- 全局统计 ----
        self.total_rows = 0
        self.total_read = 0
        self.total_write = 0
        self.hour_global = defaultdict(int)
        self.minute_global = defaultdict(int)
        self.second_global = defaultdict(int)
        self.size_welford = {'count': 0, 'mean': 0.0, 'M2': 0.0}
        self.processed_members = []

    def ingest_chunk(self, chunk: pd.DataFrame, member_name: str):
        """
        摄入一个数据块并增量聚合。

        Args:
            chunk: I/O 轨迹 DataFrame，包含 device_id, operation, offset, size, timestamp 列
            member_name: tar 成员文件名

        Returns:
            True 继续处理，False 达到 max_rows 限制应停止
        """
        if chunk is None or len(chunk) == 0:
            return True

        # 防御：非 I/O trace 格式（少于5列）直接跳过
        if len(chunk.columns) < len(IO_TRACE_COLUMNS):
            logger.debug(f"跳过非 I/O trace 格式 (列数={len(chunk.columns)}): {member_name}")
            return True

        df = chunk.copy()

        # ---------- 步骤1: 列名规范化 ----------
        if list(df.columns) != IO_TRACE_COLUMNS:
            df = self._normalize_columns(df)

        # ---------- 步骤2: 缺失值过滤 ----------
        df = df.dropna(subset=IO_TRACE_COLUMNS).copy()

        # ---------- 步骤3: 类型转换 ----------
        df['device_id'] = df['device_id'].astype('int64')
        df['size'] = pd.to_numeric(df['size'], errors='coerce').fillna(0).astype('int64')
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').fillna(0).astype('int64')

        # ---------- 步骤4: 操作类型标准化 & 无效值过滤 ----------
        df['operation'] = df['operation'].astype(str).str.upper().str.strip()
        df = df[df['operation'].isin(['R', 'W'])]

        if len(df) == 0:
            return True

        # === 采样优化：跳过部分行 ===
        if self.sample_ratio < 1.0:
            df = df.sample(frac=self.sample_ratio, random_state=42)
            if len(df) == 0:
                return True

        # === max_rows 检查 ===
        if self.max_rows > 0 and self.total_rows >= self.max_rows:
            return False

        # 如果加上当前块会超 max_rows，按比例截断
        if self.max_rows > 0 and self.total_rows + len(df) > self.max_rows:
            needed = self.max_rows - self.total_rows
            if needed <= 0:
                return False
            df = df.iloc[:needed]

        # 计算派生列（纯整数运算，避免 pd.to_datetime 开销）
        df['time_window_key'] = (df['timestamp'] // (self.time_window_sec * 1_000_000)).astype('int64')
        df['is_read'] = (df['operation'] == 'R').astype('int8')
        df['is_write'] = (df['operation'] == 'W').astype('int8')
        df['size_kb'] = df['size'] / 1024.0
        df['hour'] = (df['timestamp'] // 3_600_000_000).astype('int8') % 24

        # ---- 1. 按设备聚合（向量化版本）----
        for device_id, group in df.groupby('device_id'):
            dev_id = int(device_id)
            ds = self.dev_stats[dev_id]

            ds['total_count'] += len(group)
            ds['read_count'] += int(group['is_read'].sum())
            ds['write_count'] += int(group['is_write'].sum())
            ds['total_bytes'] += int(group['size'].sum())
            ds['sum_size'] += float(group['size'].sum())
            ds['sum_size_sq'] += float((group['size'] ** 2).sum())

            ts_min = int(group['timestamp'].min())
            ts_max = int(group['timestamp'].max())
            if ds['first_active_us'] is None or ts_min < ds['first_active_us']:
                ds['first_active_us'] = ts_min
            if ds['last_active_us'] is None or ts_max > ds['last_active_us']:
                ds['last_active_us'] = ts_max

            # 窗口计数（value_counts 替代 Python 循环）
            wc = group['time_window_key'].value_counts()
            for wk, cnt in wc.items():
                ds['window_counts'][int(wk)] += int(cnt)
            if len(ds['window_counts']) > 500:
                ds['window_counts'] = defaultdict(
                    int,
                    sorted(ds['window_counts'].items(), key=lambda x: x[1], reverse=True)[:400]
                )

            # 小时分布（value_counts 替代 Python 循环）
            hc = group['hour'].value_counts()
            for h, cnt in hc.items():
                ds['hour_counts'][int(h)] += int(cnt)

        # ---- 2. 全局时间序列聚合 ----
        for wk, group in df.groupby('time_window_key'):
            wk_int = int(wk)
            ts_g = self.ts_global[wk_int]
            ts_g['total_count'] += len(group)
            ts_g['read_count'] += int(group['is_read'].sum())
            ts_g['write_count'] += int(group['is_write'].sum())
            ts_g['total_size_kb'] += float(group['size_kb'].sum())
            ts_g['sum_size_kb'] += float(group['size_kb'].sum())
            # 去重设备计数：最大保留5000个ID/窗口，超过停止添加（仅失去精确度）
            if len(ts_g['distinct_devices']) < 5000:
                for d in group['device_id'].unique():
                    ts_g['distinct_devices'].add(int(d))

        # ---- 3. 设备×时间序列（仅 Top N 活跃设备）----
        if self.top_device_ts is None or self.top_device_ts > 0:
            top_devs = self._get_top_devices(self.top_device_ts or 50)
            for (device_id, wk), group in df.groupby(['device_id', 'time_window_key']):
                dev_id = int(device_id)
                if dev_id not in top_devs:
                    continue
                wk_int = int(wk)
                ts_d = self.ts_device[dev_id][wk_int]
                ts_d['total_count'] += len(group)
                ts_d['read_count'] += int(group['is_read'].sum())
                ts_d['write_count'] += int(group['is_write'].sum())
                ts_d['total_size_kb'] += float(group['size_kb'].sum())
                ts_d['sum_size_kb'] += float(group['size_kb'].sum())

        # ---- 4. 热点块聚合 ----
        df['block_id'] = (df['offset'] // BLOCK_SIZE_MB).astype('int64')
        for (device_id, block_id), group in df.groupby(['device_id', 'block_id']):
            key = f"{int(device_id)}:{int(block_id)}"
            bs = self.block_stats[key]
            bs['access_count'] += len(group)
            bs['read_count'] += int(group['is_read'].sum())
            bs['write_count'] += int(group['is_write'].sum())
            bs['total_bytes'] += int(group['size'].sum())
            bs['sum_size'] += float(group['size'].sum())

        # 定期清理冷 block（每 10 次调用清理一次）
        self._block_cleanup_counter += 1
        if self._block_cleanup_counter >= 10:
            self._cleanup_cold_blocks()
            self._block_cleanup_counter = 0

        # ---- 5. 全局统计 ----
        self.total_rows += len(df)
        self.total_read += int(df['is_read'].sum())
        self.total_write += int(df['is_write'].sum())

        # 小时分布（value_counts 替代循环）
        for h, cnt in df['hour'].value_counts().items():
            self.hour_global[int(h)] += int(cnt)

        # 分钟分布（整数运算替代 pd.to_datetime + strftime）
        if not self.skip_second_dist:
            minute_of_day = (df['timestamp'] // 60_000_000).astype('int32') % 1440
            for m, cnt in minute_of_day.value_counts().items():
                self.minute_global[f"{m // 60:02d}:{m % 60:02d}"] += int(cnt)

        # Welford's batch 更新（numpy 向量化替代 Python 逐行循环）
        sizes = df['size'].values.astype(np.float64)
        n_old = self.size_welford['count']
        n_new = len(sizes)
        if n_new > 0:
            mean_old = self.size_welford['mean']
            mean_new = np.mean(sizes)
            self.size_welford['count'] = n_old + n_new
            delta = mean_new - mean_old
            self.size_welford['mean'] = mean_old + delta * n_new / self.size_welford['count']
            # batch M2 = sum((x - mean_new)^2) = sum(x^2) - n * mean_new^2
            batch_m2 = np.sum((sizes - mean_new) ** 2)
            self.size_welford['M2'] += batch_m2 + delta ** 2 * n_old * n_new / self.size_welford['count']

        return self.max_rows == 0 or self.total_rows < self.max_rows

    def _get_top_devices(self, n):
        """获取当前请求最多的 Top N 设备 ID 集合"""
        sorted_devs = sorted(
            self.dev_stats.items(),
            key=lambda x: x[1]['total_count'], reverse=True
        )[:n]
        return {d[0] for d in sorted_devs}

    def _cleanup_cold_blocks(self):
        """清理低于阈值 25% 的冷门 block，减少内存占用"""
        if len(self.block_stats) < 50000:
            return
        to_remove = [
            k for k, v in self.block_stats.items()
            if v['access_count'] < self.hotspot_threshold * 0.25
        ]
        for k in to_remove:
            del self.block_stats[k]
        if to_remove:
            logger.debug(f"清理了 {len(to_remove)} 个冷门 block，剩余 {len(self.block_stats)} 个")

    def prune_block_stats(self, max_entries: int = 100000):
        """激进裁剪：仅保留访问次数最高的 max_entries 个 block，释放内存"""
        if len(self.block_stats) <= max_entries:
            return
        # 按 access_count 排序，只保留 Top N
        sorted_items = sorted(self.block_stats.items(),
                              key=lambda x: x[1]['access_count'], reverse=True)
        keep_keys = {k for k, v in sorted_items[:max_entries]}
        removed = 0
        for k in list(self.block_stats.keys()):
            if k not in keep_keys:
                del self.block_stats[k]
                removed += 1
        if removed > 0:
            logger.info(f"block_stats 激进裁剪: 移除 {removed} 个冷门block, "
                        f"保留 {len(self.block_stats)} 个热点block "
                        f"(阈值={self.hotspot_threshold})")

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """规范化列名为标准 I/O trace 列名"""
        if len(df.columns) >= 5:
            # 检查前5列是否为数字序号
            col_strs = [str(c).strip() for c in df.columns[:5]]
            if col_strs == ['0', '1', '2', '3', '4']:
                df.columns = list(IO_TRACE_COLUMNS) + list(df.columns[5:])
                return df[IO_TRACE_COLUMNS]
            # 尝试按位置取前5列
            df = df.iloc[:, :5].copy()
            df.columns = IO_TRACE_COLUMNS
        return df

    # ---- 序列化 / 反序列化 ----

    def to_checkpoint_state(self) -> dict:
        """导出当前聚合状态为可序列化字典"""
        return {
            'total_rows_processed': self.total_rows,
            'processed_members': list(self.processed_members),
            'dev_stats': self._serialize_dev_stats(),
            'ts_global': self._serialize_ts(self.ts_global),
            'ts_device': self._serialize_ts_device(),
            'block_stats': dict(self.block_stats),
            'global': {
                'total_read': self.total_read,
                'total_write': self.total_write,
                'hour_global': dict(self.hour_global),
                'minute_global': dict(self.minute_global),
                'second_global': dict(self.second_global),
                'size_welford': self.size_welford,
            }
        }

    def from_checkpoint_state(self, state: dict):
        """从 checkpoint 恢复聚合状态"""
        self.total_rows = state.get('total_rows_processed', 0)
        self.processed_members = list(state.get('processed_members', []))
        self.total_read = state.get('global', {}).get('total_read', 0)
        self.total_write = state.get('global', {}).get('total_write', 0)
        self.hour_global = defaultdict(int, state.get('global', {}).get('hour_global', {}))
        self.minute_global = defaultdict(int, state.get('global', {}).get('minute_global', {}))
        self.second_global = defaultdict(int, state.get('global', {}).get('second_global', {}))
        self.size_welford = state.get('global', {}).get('size_welford',
                                                        {'count': 0, 'mean': 0.0, 'M2': 0.0})

        # 恢复 device stats
        for dev_id_str, ds in state.get('dev_stats', {}).items():
            dev_id = int(dev_id_str)
            self.dev_stats[dev_id] = {
                'total_count': ds.get('total_count', 0),
                'read_count': ds.get('read_count', 0),
                'write_count': ds.get('write_count', 0),
                'total_bytes': ds.get('total_bytes', 0),
                'sum_size': ds.get('sum_size', 0.0),
                'sum_size_sq': ds.get('sum_size_sq', 0.0),
                'first_active_us': ds.get('first_active_us'),
                'last_active_us': ds.get('last_active_us'),
                'window_counts': defaultdict(int, {int(k): v for k, v in ds.get('window_counts', {}).items()}),
                'hour_counts': defaultdict(int, {int(k): v for k, v in ds.get('hour_counts', {}).items()}),
                'last_offset': ds.get('last_offset'),
                'sequential_pairs': ds.get('sequential_pairs', 0),
                'total_pairs': ds.get('total_pairs', 0),
            }

        # 恢复时间序列
        for wk_str, v in state.get('ts_global', {}).items():
            wk = int(wk_str)
            self.ts_global[wk] = {
                'total_count': v.get('total_count', 0),
                'read_count': v.get('read_count', 0),
                'write_count': v.get('write_count', 0),
                'total_size_kb': v.get('total_size_kb', 0.0),
                'sum_size_kb': v.get('sum_size_kb', 0.0),
                'distinct_devices': set(v.get('distinct_devices', [])),
            }

        for dev_str, ts_data in state.get('ts_device', {}).items():
            dev_id = int(dev_str)
            # 使用与 reset() 相同的 lambda 工厂函数，确保新时间窗口有默认值
            self.ts_device[dev_id] = defaultdict(lambda: {
                'total_count': 0, 'read_count': 0, 'write_count': 0,
                'total_size_kb': 0.0, 'sum_size_kb': 0.0,
            })
            for wk_str, v in ts_data.items():
                wk = int(wk_str)
                self.ts_device[dev_id][wk] = dict(v)

        # 恢复热点块
        for key, bs in state.get('block_stats', {}).items():
            self.block_stats[key] = dict(bs)

    def _serialize_dev_stats(self):
        return {
            str(did): {
                k: (dict(v) if isinstance(v, defaultdict) else v)
                for k, v in ds.items()
            }
            for did, ds in self.dev_stats.items()
        }

    def _serialize_ts(self, ts_dict):
        return {
            str(wk): {
                k: (sorted(v) if isinstance(v, set) else v)
                for k, v in vals.items()
            }
            for wk, vals in ts_dict.items()
        }

    def _serialize_ts_device(self):
        result = {}
        for dev_id, inner in self.ts_device.items():
            result[str(dev_id)] = {
                str(wk): dict(vals) for wk, vals in inner.items()
            }
        return result

    # ---- 导出最终分析结果 ----

    def get_device_profiles_df(self) -> pd.DataFrame:
        """从聚合数据导出设备画像 DataFrame"""
        rows = []
        for dev_id, ds in self.dev_stats.items():
            total_count = ds['total_count']
            if total_count == 0:
                continue
            read_count = ds['read_count']
            write_count = ds['write_count']
            active_windows = len(ds['window_counts'])
            window_sec = self.time_window_sec

            # 峰值 IOPS
            peak_iops = (max(ds['window_counts'].values()) / window_sec
                         if ds['window_counts'] else 0)

            # 峰值小时
            peak_hour = max(ds['hour_counts'], key=ds['hour_counts'].get) if ds['hour_counts'] else 0

            # 活跃跨度
            if ds['first_active_us'] and ds['last_active_us']:
                active_span_h = (ds['last_active_us'] - ds['first_active_us']) / 1e6 / 3600.0
            else:
                active_span_h = 0

            # 平均请求大小
            avg_size = (ds['sum_size'] / total_count) / 1024.0 if total_count > 0 else 0
            std_size = np.sqrt(max(0, (ds['sum_size_sq'] / total_count) -
                                   (ds['sum_size'] / total_count) ** 2)) / 1024.0

            # 平均 IOPS
            avg_iops = total_count / (active_windows * window_sec) if active_windows > 0 else 0

            rows.append({
                'device_id': dev_id,
                'total_requests': total_count,
                'total_bytes': ds['total_bytes'],
                'read_requests': read_count,
                'write_requests': write_count,
                'read_ratio': read_count / total_count if total_count > 0 else 0,
                'read_bytes': ds['total_bytes'] * read_count / total_count if total_count > 0 else 0,
                'write_bytes': ds['total_bytes'] * write_count / total_count if total_count > 0 else 0,
                'avg_request_size_kb': avg_size,
                'std_request_size_kb': std_size,
                'first_active': (pd.Timestamp(ds['first_active_us'], unit='us', tz='Asia/Shanghai')
                                 if ds['first_active_us'] else None),
                'last_active': (pd.Timestamp(ds['last_active_us'], unit='us', tz='Asia/Shanghai')
                                if ds['last_active_us'] else None),
                'active_windows': active_windows,
                'avg_iops': avg_iops,
                'peak_iops': peak_iops,
                'peak_hour': peak_hour,
                'active_span_hours': active_span_h,
            })

        df = pd.DataFrame(rows)
        if len(df) > 0:
            df = df.sort_values('total_requests', ascending=False).reset_index(drop=True)
        return df

    def get_time_series_global_df(self) -> pd.DataFrame:
        """导出全局时间序列 DataFrame"""
        rows = []
        window_sec = self.time_window_sec
        for wk_int, vals in sorted(self.ts_global.items()):
            ts_us = wk_int * window_sec * 1_000_000
            rows.append({
                'datetime': pd.Timestamp(ts_us, unit='us', tz='Asia/Shanghai'),
                'iops': vals['total_count'] / window_sec,
                'read_ops': vals['read_count'],
                'write_ops': vals['write_count'],
                'iops_read': vals['read_count'] / window_sec,
                'iops_write': vals['write_count'] / window_sec,
                'throughput_kb': vals['total_size_kb'] / window_sec,
                'avg_request_size_kb': (vals['sum_size_kb'] / vals['total_count']
                                        if vals['total_count'] > 0 else 0),
                'distinct_devices': len(vals['distinct_devices']),
                'read_ratio': (vals['read_count'] / vals['total_count']
                               if vals['total_count'] > 0 else 0),
            })
        return pd.DataFrame(rows)

    def get_time_series_device_df(self) -> pd.DataFrame:
        """导出设备级别时间序列 DataFrame"""
        rows = []
        window_sec = self.time_window_sec
        for dev_id, inner in self.ts_device.items():
            for wk_int, vals in inner.items():
                ts_us = wk_int * window_sec * 1_000_000
                rows.append({
                    'device_id': dev_id,
                    'datetime': pd.Timestamp(ts_us, unit='us', tz='Asia/Shanghai'),
                    'iops': vals['total_count'] / window_sec,
                    'read_ops': vals['read_count'],
                    'write_ops': vals['write_count'],
                    'iops_read': vals['read_count'] / window_sec,
                    'iops_write': vals['write_count'] / window_sec,
                    'throughput_kb': vals['total_size_kb'] / window_sec,
                    'avg_request_size_kb': (vals['sum_size_kb'] / vals['total_count']
                                            if vals['total_count'] > 0 else 0),
                })
        return pd.DataFrame(rows)

    def get_hotspots_df(self, top_n=None) -> pd.DataFrame:
        """导出热点块 DataFrame"""
        rows = []
        for key, bs in self.block_stats.items():
            if bs['access_count'] < self.hotspot_threshold:
                continue
            device_id, block_id = key.split(':')
            rows.append({
                'device_id': int(device_id),
                'block_id': int(block_id),
                'access_count': bs['access_count'],
                'read_count': bs['read_count'],
                'write_count': bs['write_count'],
                'total_bytes': bs['total_bytes'],
                'avg_request_size': bs['sum_size'] / bs['access_count'] if bs['access_count'] > 0 else 0,
                'block_start_offset_mb': int(block_id) * BLOCK_SIZE_MB / (1024 * 1024),
            })
        df = pd.DataFrame(rows)
        if len(df) > 0:
            df = df.sort_values('access_count', ascending=False)
            if top_n and len(df) > top_n:
                df = df.head(top_n)
        return df

    def get_access_patterns(self) -> dict:
        """从聚合数据导出访问模式"""
        total = self.total_rows
        read_ratio = self.total_read / total if total > 0 else 0

        # 请求大小统计
        wf = self.size_welford
        if wf['count'] > 0:
            std = np.sqrt(wf['M2'] / wf['count']) if wf['count'] > 1 else 0.0
        else:
            std = 0.0

        # 顺序度（所有设备的平均值）
        seq_ratios = []
        for ds in self.dev_stats.values():
            if ds['total_pairs'] > 0:
                seq_ratios.append(ds['sequential_pairs'] / ds['total_pairs'])
        seq_ratio = np.mean(seq_ratios) if seq_ratios else 0
        seq_std = np.std(seq_ratios) if seq_ratios else 0

        # 活跃窗口占比
        if self.ts_global:
            min_wk = min(self.ts_global.keys())
            max_wk = max(self.ts_global.keys())
            total_windows = max_wk - min_wk + 1
            active_windows = len(self.ts_global)
            active_ratio = active_windows / total_windows if total_windows > 0 else 0
        else:
            total_windows = 0
            active_windows = 0
            active_ratio = 0

        # 峰值/低谷小时
        peak_hour = max(self.hour_global, key=self.hour_global.get) if self.hour_global else None
        off_peak = min(self.hour_global, key=self.hour_global.get) if self.hour_global else None

        # 主要请求大小（mode）
        dominant_size = int(wf['mean']) if wf['count'] > 0 else None

        return {
            'global_read_ratio': read_ratio,
            'global_write_ratio': 1 - read_ratio,
            'request_size_stats': {
                'count': total,
                'mean': float(wf['mean']),
                'std': std,
                'min': float(wf['mean'] - 3 * std) if std > 0 else 0,
                '25%': float(wf['mean'] - 0.6745 * std) if std > 0 else float(wf['mean']),
                '50%': float(wf['mean']),
                '75%': float(wf['mean'] + 0.6745 * std) if std > 0 else float(wf['mean']),
                'max': float(wf['mean'] + 3 * std) if std > 0 else float(wf['mean']),
            },
            'dominant_request_size': dominant_size,
            'sequential_access_ratio': float(seq_ratio),
            'sequential_access_std': float(seq_std),
            'active_window_ratio': active_ratio,
            'total_time_windows': total_windows,
            'active_time_windows': active_windows,
            'peak_hour': int(peak_hour) if peak_hour is not None else None,
            'off_peak_hour': int(off_peak) if off_peak is not None else None,
            'hourly_distribution': {int(k): v for k, v in self.hour_global.items()},
            'minute_distribution': dict(self.minute_global),
            'second_distribution': dict(self.second_global),
        }


# ================================================================
# 流式处理器
# ================================================================

class StreamingTarProcessor:
    """
    流式 tar.gz 处理器

    通过 HTTP Range 下载，边下载边解压边处理。
    支持中断后从 checkpoint 恢复。
    """

    def __init__(self, url: str, work_dir: str, config: dict = None, output_dir: str = None):
        self.url = url
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.config = config or {}
        self.max_retries = self.config.get('max_retries', 3)
        self.checkpoint_interval_rows = self.config.get('checkpoint_interval_rows', 5000000)
        self.output_dir = output_dir  # 增量保存目录

        self.checkpoint = CheckpointManager(str(self.work_dir))
        # 把 load_analysis 和 streaming 参数合并传给 aggregator
        agg_config = dict(self.config.get('load_analysis', {}))
        for k in ('max_rows', 'sample_ratio', 'top_device_ts', 'skip_second_dist'):
            if k in self.config:
                agg_config[k] = self.config[k]
        self.aggregator = IncrementalAggregator(agg_config)

        # 下载状态
        self.temp_file = self.work_dir / 'streaming_temp.tar.gz'
        self.bytes_downloaded = 0
        self.decompressed_bytes = 0  # 解压后的 CSV 数据量
        self.total_file_size = None

    def get_file_size(self) -> int:
        """获取远程文件大小"""
        for attempt in range(self.max_retries):
            try:
                resp = requests.head(self.url, timeout=30)
                resp.raise_for_status()
                cl = resp.headers.get('content-length')
                if cl:
                    self.total_file_size = int(cl)
                    logger.info(f"远程文件大小: {self.total_file_size / (1024**3):.2f} GB")
                    return self.total_file_size
            except Exception as e:
                logger.warning(f"获取文件大小失败 (尝试 {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return 0

    def process(self, progress_callback=None) -> dict:
        """
        执行完整流式处理流程（真正的流式：HTTP→gzip→tar→process，不落盘）。

        流程：
        1. 检查 checkpoint，恢复聚合状态
        2. HTTP 流式下载，直接管道进 gzip 解压 → tar 逐成员处理
        3. 处理每个 csv 成员时增量聚合
        4. 定期保存 checkpoint
        5. max_gb/max_rows 达到限制时自动停止

        Returns:
            分析结果字典（device_profiles, time_series_global, hotspots, access_patterns）
        """
        logger.info("=" * 60)
        logger.info("开始流式处理 (直接流式传输，不保存原始数据)")
        logger.info("=" * 60)

        # 1. 清理旧的 temp 文件
        if self.temp_file.exists():
            old_size = self.temp_file.stat().st_size
            self.temp_file.unlink(missing_ok=True)
            logger.info(f"已清理旧临时文件 ({old_size / (1024**3):.2f} GB)")

        # 2. 读取限制配置（必须在 checkpoint 之前，日志要用）
        max_rows = self.aggregator.max_rows
        max_gb_total = self.config.get('max_gb', 0)
        if max_gb_total <= 0:
            max_gb_total = self.config.get('partial_end_gb', 0)  # 兼容旧 config.json
        max_gb_incremental = self.config.get('max_gb_incremental', 0)
        max_total_bytes = max_gb_total * (1024 ** 3) if max_gb_total > 0 else 0
        max_inc_bytes = max_gb_incremental * (1024 ** 3) if max_gb_incremental > 0 else 0

        # 3. 检查 checkpoint 并恢复聚合状态
        cp_state = self.checkpoint.load()
        if cp_state:
            self.bytes_downloaded = cp_state.get('total_bytes_downloaded', 0)
            self.decompressed_bytes = cp_state.get('decompressed_bytes', 0)
            self.aggregator.from_checkpoint_state(cp_state.get('aggregator', {}))
            # 显示 checkpoint 状态：累计值 + 总量目标 + 本次增量目标
            msg = (f"从 checkpoint 恢复: 累计已解压 {self.decompressed_bytes / (1024**3):.2f} GB, "
                   f"已聚合 {self.aggregator.total_rows:,} 行, "
                   f"{len(self.aggregator.processed_members)} 个成员已处理")
            if max_gb_total > 0:
                remaining = max_gb_total - self.decompressed_bytes / (1024**3)
                msg += f" | 总量目标 {max_gb_total:.2f} GB，剩余 {remaining:.2f} GB"
                if remaining <= 0:
                    msg += " (已达总量上限)"
            if max_gb_incremental > 0:
                msg += f" | 本次增量: {max_gb_incremental:.2f} GB"
            logger.info(msg)
        else:
            self.bytes_downloaded = 0
            self.decompressed_bytes = 0
            self.aggregator.reset()
            msg = "从零开始 — 无 checkpoint"
            if max_gb_incremental > 0:
                msg += f"（本次增量目标 {max_gb_incremental:.2f} GB，总量目标 {max_gb_total:.2f} GB）"
            logger.info(msg)

        # 记录本次运行开始时的累计值（用于计算本次增量进度百分比）
        self.decompressed_at_start = self.decompressed_bytes

        # 4. 检查是否已达上限
        if max_rows > 0 and self.aggregator.total_rows >= max_rows:
            logger.info(f"已达目标行数 {max_rows:,}，跳过处理")
            return self._build_final_results()
        if max_total_bytes > 0 and self.decompressed_bytes >= max_total_bytes:
            logger.info(f"累计已达总量目标 {max_gb_total} GB，跳过处理")
            return self._build_final_results()

        # 5. 获取远程文件大小
        self.total_file_size = self.get_file_size()
        if not self.total_file_size:
            logger.warning("无法获取文件大小")

        # 6. 真正的流式处理：HTTP → gzip → tar → 增量聚合
        self._true_stream_process(progress_callback, max_gb_total, max_gb_incremental)

        # 7. 导出最终结果
        results = self._build_final_results()
        logger.info(f"流式处理完成: {self.aggregator.total_rows:,} 行, "
                    f"{len(self.aggregator.dev_stats)} 个设备, "
                    f"累计解压 {self.decompressed_bytes / (1024**3):.2f} GB")

        return results

    def _true_stream_process(self, progress_callback=None, max_gb_total=0, max_gb_incremental=0):
        """
        真正的流式处理管道：HTTP → gzip → tar → 逐成员聚合。
        不保存任何原始数据到磁盘。

        max_gb_total: 累计总量目标（跨运行累积，如 20GB）
        max_gb_incremental: 本次运行增量目标（如 0.1GB），百分比以此计算
        
        断点续传策略：重新下载，跳过已处理的 tar 成员。
        """
        processed = set(self.aggregator.processed_members)
        max_total_bytes = max_gb_total * (1024 ** 3) if max_gb_total > 0 else 0
        max_inc_bytes = max_gb_incremental * (1024 ** 3) if max_gb_incremental > 0 else 0
        # 如果没有增量限制，用总量作为本次限制（兼容旧行为）
        if max_inc_bytes <= 0 and max_total_bytes > 0:
            max_inc_bytes = max_total_bytes
        # 如果增量限制超过总量剩余，裁剪
        if max_total_bytes > 0 and max_inc_bytes > 0:
            remaining_total = max_total_bytes - self.decompressed_at_start
            max_inc_bytes = min(max_inc_bytes, remaining_total)
        rows_since_checkpoint = 0
        rows_since_incremental = 0
        member_count = 0
        first_save_done = False  # 第一次增量保存标志
        decomp_at_start = self.decompressed_at_start  # snapshot for this run
        
        if processed:
            logger.info(f"断点续传：将重新下载数据，跳过 {len(processed)} 个已处理成员")

        # ---- 下载进度后台线程（解决下载期间无反馈的问题）----
        download_reader = [None]  # mutable ref for closure

        def _progress_thread():
            sleep_sec = 5      # 前几次频繁汇报
            fast_count = 0
            last_mb = 0
            while download_reader[0] is not None:
                time.sleep(sleep_sec)
                rd = download_reader[0]
                if rd is None:
                    break
                mb_now = rd.bytes_total / (1024 ** 2)
                # 前 5 次每 5 秒汇报一次（只要有进展），之后每 30 秒+10MB
                if fast_count < 5:
                    if mb_now > last_mb:
                        logger.info(f"  下载进度: {mb_now:.1f} MB "
                                    f"| 累计解压: {(decomp_at_start + self.decompressed_bytes) / (1024**3):.3f} GB"
                                    f" | 已聚合: {self.aggregator.total_rows:,} 行")
                        last_mb = mb_now
                    fast_count += 1
                else:
                    sleep_sec = 30
                    if mb_now > last_mb + 10:
                        logger.info(f"  下载进度: {mb_now:.0f} MB "
                                    f"| 累计解压: {(decomp_at_start + self.decompressed_bytes) / (1024**3):.3f} GB"
                                    f" | 已聚合: {self.aggregator.total_rows:,} 行")
                        last_mb = mb_now

        t = threading.Thread(target=_progress_thread, daemon=True)
        t.start()

        for attempt in range(self.max_retries):
            try:
                response = requests.get(self.url, stream=True, timeout=60)
                response.raise_for_status()
                logger.info("HTTP 连接已建立，开始接收压缩数据...")
                
                # 获取总大小用于进度
                cl = response.headers.get('content-length')
                if cl:
                    self.total_file_size = max(int(cl), self.total_file_size or 0)

                bytes_read = 0
                
                # 构造 HTTP 流 → gzip 管道
                response_iter = response.iter_content(chunk_size=65536)
                
                class _IterReader:
                    """将 HTTP chunk 迭代器包装成 file-like read() 接口"""
                    def __init__(_self):
                        _self.buf = b''
                        _self.done = False
                        _self.bytes_total = 0
                    def read(_self, size=-1):
                        if _self.done and not _self.buf:
                            return b''
                        while size < 0 or len(_self.buf) < size:
                            try:
                                chunk = next(response_iter)
                                if not chunk:
                                    _self.done = True
                                    break
                                _self.buf += chunk
                                _self.bytes_total += len(chunk)
                            except StopIteration:
                                _self.done = True
                                break
                        if size < 0:
                            data, _self.buf = _self.buf, b''
                        elif _self.buf:
                            data, _self.buf = _self.buf[:size], _self.buf[size:]
                        else:
                            data = b''
                        return data
                
                reader = _IterReader()
                download_reader[0] = reader

                info_parts = ["正在流式下载并解析数据"]
                if max_gb_incremental > 0:
                    info_parts.append(f"本次限制 {max_gb_incremental:.2f} GB")
                if max_gb_total > 0:
                    info_parts.append(f"累计目标 {max_gb_total:.2f} GB")
                info_parts.append("（前25秒每5秒汇报，之后每30秒）")
                logger.info('，'.join(info_parts))
                
                with gzip.GzipFile(fileobj=reader) as gz:
                    with tarfile.open(fileobj=gz, mode='r|') as tar:
                        for member in tar:
                            member_count += 1
                            
                            # 跳过已处理成员
                            if member.name in processed:
                                logger.debug(f"跳过已处理成员 [{member_count}]: {member.name}")
                                continue

                            # 更新下载进度
                            self.bytes_downloaded = reader.bytes_total
                            bytes_read = reader.bytes_total

                            if member.isfile() and member.name.endswith('.csv') \
                                    and 'device_size' not in member.name.lower():
                                logger.info(f"处理 [{member_count}]: {member.name} "
                                            f"({member.size / (1024**2):.1f} MB)"
                                            f"{f' [本次限制 {max_gb_incremental:.2f} GB]' if max_gb_incremental > 0 else ''}")
                                rows_before = self.aggregator.total_rows
                                # 传入本次增量限制字节数，用于进度百分比
                                should_continue, actual_bytes = self._process_tar_member(
                                    tar, member, max_inc_bytes, max_gb_incremental, decomp_at_start
                                )
                                rows_processed_val = self.aggregator.total_rows - rows_before
                                rows_since_checkpoint += rows_processed_val
                                rows_since_incremental += rows_processed_val
                                self.decompressed_bytes += actual_bytes  # 用实际读取字节数
                                self.aggregator.processed_members.append(member.name)

                                # 定期保存 checkpoint 已移除：仅在阶段1全部完成后保存最终 checkpoint
                                # rows_since_checkpoint 仅用于 finally 块判断是否有新数据

                                # 增量保存分析结果到 data/device_analysis/（用户可见）
                                # 第一个成员处理完立即保存；后续每 200 万行
                                inc_threshold = 2_000_000
                                do_save = False
                                if self.output_dir:
                                    if not first_save_done and rows_since_incremental > 0:
                                        do_save = True
                                        first_save_done = True
                                    elif rows_since_incremental >= inc_threshold:
                                        do_save = True
                                if do_save:
                                    self._incremental_save()
                                    rows_since_incremental = 0

                                # 进度回调
                                if progress_callback:
                                    progress_callback(self.bytes_downloaded, max_inc_bytes or 1,
                                                      self.aggregator.total_rows, 'stream')

                                # 检查限制
                                max_rows_val = self.aggregator.max_rows
                                if max_rows_val > 0 and self.aggregator.total_rows >= max_rows_val:
                                    logger.info(f"已达目标行数 {max_rows_val:,}，停止处理")
                                    return
                                # 本次增量限制（优先级最高）
                                if max_inc_bytes > 0 and (self.decompressed_bytes - decomp_at_start) >= max_inc_bytes:
                                    run_done = (self.decompressed_bytes - decomp_at_start) / (1024**3)
                                    cum = self.decompressed_bytes / (1024**3)
                                    logger.info(f"本次增量已达 {max_gb_incremental:.2f} GB "
                                                f"（本次实际 {run_done:.3f} GB，累计 {cum:.3f} GB），停止处理")
                                    return
                                # 总量限制（兜底检查）
                                if max_total_bytes > 0 and self.decompressed_bytes >= max_total_bytes:
                                    logger.info(f"累计已达总量目标 {max_gb_total:.2f} GB，停止处理")
                                    return
                                if not should_continue:
                                    return

                            elif member.isfile() and member.name.endswith('.json'):
                                logger.info(f"扫描 [{member_count}]: {member.name} (JSON, 跳过)")
                            elif member.isfile():
                                logger.info(f"扫描 [{member_count}]: {member.name} (非CSV, 跳过)")

                logger.info(f"数据流传输完成，共读取 {reader.bytes_total / (1024**3):.2f} GB, "
                            f"{member_count} 个 tar 成员")
                break

            except Exception as e:
                logger.warning(f"流式处理失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
            finally:
                download_reader[0] = None  # 停止进度线程
                # 中间 checkpoint 已移除，最终 checkpoint 在 save_aggregation() 中保存

    def _process_tar_member(self, tar: tarfile.TarFile, member: tarfile.TarInfo,
                            max_inc_bytes=0, max_gb_inc=0, decomp_at_start=0):
        """处理单个 tar 成员中的 CSV 数据。
        返回 (should_continue, bytes_read_total)：
          - should_continue=True 表示继续处理下一个成员
          - bytes_read_total 是实际读取的字节数（用于准确追踪解压大小）
        max_inc_bytes: 本次增量限制字节数（用于进度百分比分母和停止条件）
        max_gb_inc: 本次增量限制 GB 值（仅用于日志显示）
        decomp_at_start: 本次运行开始时的累计解压字节数
        """
        fileobj = tar.extractfile(member)
        if fileobj is None:
            return True, 0

        try:
            # 流式分块处理：不读取整个文件（可能是几百 GB），
            # 改为每次读固定大小的块，按行边界切割后喂给 pandas
            pending = b''
            block_size = 20 * 1024 * 1024  # 20 MB 每次
            bytes_processed = 0
            last_log_mb = 0
            last_log_time = time.time()

            while True:
                data = fileobj.read(block_size)
                if not data:
                    break

                bytes_processed += len(data)
                pending += data

                # 每 20 MB 或 15 秒打印一次进度（快速反馈）
                mb_processed = bytes_processed // (1024 * 1024)
                now = time.time()
                if mb_processed >= last_log_mb + 20 or (now - last_log_time >= 15 and mb_processed > last_log_mb):
                    last_log_mb = mb_processed
                    last_log_time = now
                    # 百分比基于本次增量目标，不再基于整个文件大小（768GB → 0.0%）
                    if max_inc_bytes > 0:
                        pct = bytes_processed / max_inc_bytes * 100
                        pct_str = f"{pct:.1f}% (本次目标 {max_gb_inc:.2f} GB)"
                    else:
                        pct_str = ""
                    cum_now = (decomp_at_start + self.decompressed_bytes + bytes_processed) / (1024**3)
                    logger.info(f"  {member.name}: 已解压 {mb_processed} MB "
                                f"({pct_str}) | 累计 {cum_now:.3f} GB | 已聚合 {self.aggregator.total_rows:,} 行")

                # 达到本次增量限制 → 停止（不继续读更多数据）
                if max_inc_bytes > 0 and (self.decompressed_bytes - decomp_at_start) + bytes_processed >= max_inc_bytes:
                    cum_now = (self.decompressed_bytes + bytes_processed) / (1024**3)
                    logger.info(f"  {member.name}: 本次增量已达 {max_gb_inc:.2f} GB "
                                f"（累计 {cum_now:.3f} GB），停止读取当前成员")
                    return True, bytes_processed

                # 找到最后一个完整行
                last_nl = pending.rfind(b'\n')
                if last_nl == -1:
                    continue  # 还没完整行，继续读

                # 拆分：完整部分去处理，不完整部分保留到下一轮
                complete = pending[:last_nl + 1]
                pending = pending[last_nl + 1:]

                buf = io.BytesIO(complete)
                for chunk in pd.read_csv(buf, chunksize=100000, header=None,
                                         low_memory=False):
                    if len(chunk) > 0:
                        should_continue = self.aggregator.ingest_chunk(chunk, member.name)
                        if not should_continue:
                            return False, bytes_processed

            # 处理最后残留的行
            if pending.strip():
                buf = io.BytesIO(pending)
                for chunk in pd.read_csv(buf, chunksize=100000, header=None,
                                         low_memory=False):
                    if len(chunk) > 0:
                        should_continue = self.aggregator.ingest_chunk(chunk, member.name)
                        if not should_continue:
                            return False, bytes_processed

            return True, bytes_processed
        except Exception as e:
            logger.warning(f"处理 {member.name} 时出错: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return True, 0

    def _save_checkpoint(self):
        """保存当前 checkpoint（失败不中断主流程）"""
        try:
            state = {
                'total_bytes_downloaded': self.bytes_downloaded,
                'decompressed_bytes': self.decompressed_bytes,
                'total_file_size': self.total_file_size,
                'aggregator': self.aggregator.to_checkpoint_state(),
            }
            self.checkpoint.save(state)
        except Exception as e:
            logger.warning(f"Checkpoint 保存失败（不影响处理，下次将重新处理部分数据）: {e}")

    def _incremental_save(self):
        """增量保存当前分析结果到 output_dir（用户随时可查看）并释放内存"""
        if not self.output_dir:
            return
        try:
            from src.device_load_analysis.load_reporter import LoadReporter
            os.makedirs(self.output_dir, exist_ok=True)
            results = self._build_final_results()
            reporter = LoadReporter(save_dir=self.output_dir)
            reporter.save_results(results, self.output_dir)
            logger.info(f"增量保存完成: {self.output_dir} "
                        f"({self.aggregator.total_rows:,} 行, "
                        f"{len(self.aggregator.dev_stats):,} 设备, "
                        f"{len(self.aggregator.block_stats):,} blocks)")
            # 保存后立即裁剪 block_stats 释放内存（保留 Top 100k 热点 block）
            self.aggregator.prune_block_stats(max_entries=100000)
        except Exception as e:
            logger.warning(f"增量保存失败（不影响主流程，下次 checkpoint 重试）: {e}")
            import traceback
            logger.warning(traceback.format_exc())

    def _build_final_results(self) -> dict:
        """构建最终分析结果（仅保留阶段2需要的5个核心数据集）"""
        return {
            'device_profiles': self.aggregator.get_device_profiles_df(),
            'time_series_global': self.aggregator.get_time_series_global_df(),
            'time_series_device': self.aggregator.get_time_series_device_df(),
            'hotspots': self.aggregator.get_hotspots_df(top_n=500),
            'access_patterns': self.aggregator.get_access_patterns(),
            'total_rows_processed': self.aggregator.total_rows,
            'total_devices': len(self.aggregator.dev_stats),
        }

    def cleanup(self):
        """清理所有临时文件和中间目录"""
        # 清理临时下载文件
        if self.temp_file.exists():
            try:
                self.temp_file.unlink()
                logger.info(f"已清理临时文件: {self.temp_file}")
            except Exception as e:
                logger.warning(f"清理临时文件失败: {e}")

        # 清理 checkpoint 目录
        self.checkpoint.clear()
        self._cleanup_work_dir()

        # 清理空的 data 子目录
        self._cleanup_empty_data_dirs()

    def _cleanup_work_dir(self):
        """清理 checkpoint 工作目录（如已空则删除）"""
        import shutil
        work_dir = self.work_dir
        if not work_dir.exists():
            return
        try:
            # 先删 checkpoint 文件
            if self.checkpoint.exists():
                self.checkpoint.clear()
            # 如果目录已空，删除目录
            remaining = list(work_dir.iterdir())
            if not remaining:
                work_dir.rmdir()
                logger.info(f"已清理工作目录: {work_dir}")
        except Exception as e:
            logger.debug(f"清理工作目录时忽略: {e}")

    @staticmethod
    def _cleanup_empty_data_dirs():
        """清理 data/ 下空的子目录（processed, raw 等）"""
        import shutil
        data_dir = Path('data')
        if not data_dir.exists():
            return
        for sub in list(data_dir.iterdir()):
            if sub.is_dir():
                try:
                    remaining = list(sub.iterdir())
                    if not remaining:
                        # 忽略 device_analysis（阶段2 需要）
                        if sub.name == 'device_analysis':
                            continue
                        sub.rmdir()
                        logger.info(f"已清理空目录: {sub}")
                except Exception:
                    pass

    def save_aggregation(self, save_dir: str) -> dict:
        """
        阶段1: 仅保存聚合数据（CSV + JSON），不生成可视化和报告。

        Returns:
            分析结果字典
        """
        from src.device_load_analysis.load_reporter import LoadReporter

        results = self._build_final_results()
        reporter = LoadReporter(save_dir=save_dir)

        profiles = results['device_profiles']
        patterns = results['access_patterns']
        ts_global = results['time_series_global']

        # 打印摘要
        reporter.print_summary(profiles, patterns, ts_global)

        # 仅保存数据文件（CSV/JSON），不生成图表
        # 使用原子性保存：先保存到临时目录，成功后再移动
        try:
            reporter.save_results(results, save_dir)
        except Exception as e:
            logger.error(f"聚合数据保存失败，保留 checkpoint 以便恢复: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # 保存失败时不清理 checkpoint，保留恢复能力
            return results

        # 验证关键文件是否已成功写入
        required_files = ['device_profiles.csv', 'time_series_global.csv', 'access_patterns.json']
        missing = [f for f in required_files if not os.path.exists(os.path.join(save_dir, f))]
        if missing:
            logger.error(f"关键文件缺失: {missing}，保留 checkpoint 以便恢复")
            return results

        # 保存最终 checkpoint（保留，不删除，以便后续查看或恢复）
        self._save_checkpoint()
        logger.info("数据保存验证通过，最终 checkpoint 已保留")

        return results

    def generate_visualization(self, data_dir: str, viz_dir: str = None) -> dict:
        """
        阶段2: 从已保存的聚合数据生成可视化和报告（可反复重跑）。

        Args:
            data_dir: 聚合数据目录（CSV/JSON 所在位置）
            viz_dir:  图表/报告输出目录（默认与 data_dir 相同）

        Returns:
            分析结果字典
        """
        from src.device_load_analysis.load_reporter import LoadReporter

        if viz_dir is None:
            viz_dir = data_dir

        # 从保存的 CSV/JSON 加载聚合数据
        results = self._load_aggregation_from_dir(data_dir)
        if results is None:
            logger.error("无法加载聚合数据，请先运行阶段1")
            return None

        reporter = LoadReporter(save_dir=viz_dir)
        profiles = results['device_profiles']
        patterns = results['access_patterns']
        ts_global = results['time_series_global']

        # 打印摘要
        reporter.print_summary(profiles, patterns, ts_global)

        # 生成可视化图表 → {viz_dir}/load_charts/
        reporter.visualize_load(results, viz_dir)

        # 生成 HTML 报告 → {viz_dir}/load_report.html
        reporter.generate_html_report(results, viz_dir)

        logger.info(f"可视化和报告已生成: {viz_dir}")
        return results

    def save_final_results(self, save_dir: str):
        """完整保存（阶段1+阶段2，兼容旧接口）"""
        self.save_aggregation(save_dir)
        self.generate_visualization(save_dir, save_dir)

    def _load_aggregation_from_dir(self, save_dir: str) -> dict:
        """从已保存的目录加载聚合结果"""
        results = {}
        try:
            profiles_path = os.path.join(save_dir, 'device_profiles.csv')
            ts_global_path = os.path.join(save_dir, 'time_series_global.csv')
            ts_device_path = os.path.join(save_dir, 'time_series_device.csv')
            hotspots_path = os.path.join(save_dir, 'hotspots.csv')
            patterns_path = os.path.join(save_dir, 'access_patterns.json')

            if os.path.exists(profiles_path):
                results['device_profiles'] = pd.read_csv(profiles_path)
            if os.path.exists(ts_global_path):
                df = pd.read_csv(ts_global_path)
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'])
                results['time_series_global'] = df
            if os.path.exists(ts_device_path):
                df = pd.read_csv(ts_device_path)
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'])
                results['time_series_device'] = df
            if os.path.exists(hotspots_path):
                results['hotspots'] = pd.read_csv(hotspots_path)
            if os.path.exists(patterns_path):
                with open(patterns_path, 'r', encoding='utf-8') as f:
                    results['access_patterns'] = json.load(f)

            # 基本校验
            if 'device_profiles' not in results and 'time_series_global' not in results:
                logger.warning("聚合目录中数据不完整（缺少 device_profiles 和 time_series_global）")
                return None

            logger.info(f"从 {save_dir} 加载聚合数据完成")
            return results
        except Exception as e:
            logger.error(f"加载聚合数据失败: {e}")
            return None
