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
import numpy as np
import pandas as pd
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

IO_TRACE_COLUMNS = ['device_id', 'operation', 'offset', 'size', 'timestamp']
BLOCK_SIZE_MB = 1024 * 1024  # 1MB 磁盘块大小


class IncrementalAggregator:
    """增量聚合器：逐块处理 I/O 轨迹数据，累积统计指标"""

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
            'window_counts': defaultdict(int),
            'hour_counts': defaultdict(int),
            'last_offset': None,
            'sequential_pairs': 0,
            'total_pairs': 0,
        })
        # ---- 时间序列聚合 ----
        self.ts_global = defaultdict(lambda: {
            'total_count': 0, 'read_count': 0, 'write_count': 0,
            'total_size_kb': 0.0, 'sum_size_kb': 0.0,
            'distinct_devices': set(),
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
        """摄入一个数据块并增量聚合。返回 True 继续，False 达到限制应停止。"""
        if chunk is None or len(chunk) == 0:
            return True
        if len(chunk.columns) < len(IO_TRACE_COLUMNS):
            logger.debug(f"跳过非 I/O trace 格式 (列数={len(chunk.columns)}): {member_name}")
            return True

        df = chunk.copy()

        if list(df.columns) != IO_TRACE_COLUMNS:
            df = self._normalize_columns(df)

        df = df.dropna(subset=IO_TRACE_COLUMNS).copy()
        df['device_id'] = df['device_id'].astype('int64')
        df['size'] = pd.to_numeric(df['size'], errors='coerce').fillna(0).astype('int64')
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').fillna(0).astype('int64')
        df['operation'] = df['operation'].astype(str).str.upper().str.strip()
        df = df[df['operation'].isin(['R', 'W'])]

        if len(df) == 0:
            return True

        if self.sample_ratio < 1.0:
            df = df.sample(frac=self.sample_ratio, random_state=42)
            if len(df) == 0:
                return True

        if self.max_rows > 0 and self.total_rows >= self.max_rows:
            return False
        if self.max_rows > 0 and self.total_rows + len(df) > self.max_rows:
            needed = self.max_rows - self.total_rows
            if needed <= 0:
                return False
            df = df.iloc[:needed]

        df['time_window_key'] = (df['timestamp'] // (self.time_window_sec * 1_000_000)).astype('int64')
        df['is_read'] = (df['operation'] == 'R').astype('int8')
        df['is_write'] = (df['operation'] == 'W').astype('int8')
        df['size_kb'] = df['size'] / 1024.0
        df['hour'] = (df['timestamp'] // 3_600_000_000).astype('int8') % 24

        # ---- 1. 按设备聚合 ----
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

            wc = group['time_window_key'].value_counts()
            for wk, cnt in wc.items():
                ds['window_counts'][int(wk)] += int(cnt)
            if len(ds['window_counts']) > 500:
                ds['window_counts'] = defaultdict(
                    int,
                    sorted(ds['window_counts'].items(), key=lambda x: x[1], reverse=True)[:400]
                )

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

        self._block_cleanup_counter += 1
        if self._block_cleanup_counter >= 10:
            self._cleanup_cold_blocks()
            self._block_cleanup_counter = 0

        # ---- 5. 全局统计 ----
        self.total_rows += len(df)
        self.total_read += int(df['is_read'].sum())
        self.total_write += int(df['is_write'].sum())

        for h, cnt in df['hour'].value_counts().items():
            self.hour_global[int(h)] += int(cnt)

        if not self.skip_second_dist:
            minute_of_day = (df['timestamp'] // 60_000_000).astype('int32') % 1440
            for m, cnt in minute_of_day.value_counts().items():
                self.minute_global[f"{m // 60:02d}:{m % 60:02d}"] += int(cnt)

        sizes = df['size'].values.astype(np.float64)
        n_old = self.size_welford['count']
        n_new = len(sizes)
        if n_new > 0:
            mean_old = self.size_welford['mean']
            mean_new = np.mean(sizes)
            self.size_welford['count'] = n_old + n_new
            delta = mean_new - mean_old
            self.size_welford['mean'] = mean_old + delta * n_new / self.size_welford['count']
            batch_m2 = np.sum((sizes - mean_new) ** 2)
            self.size_welford['M2'] += batch_m2 + delta ** 2 * n_old * n_new / self.size_welford['count']

        return self.max_rows == 0 or self.total_rows < self.max_rows

    def _get_top_devices(self, n):
        sorted_devs = sorted(
            self.dev_stats.items(),
            key=lambda x: x[1]['total_count'], reverse=True
        )[:n]
        return {d[0] for d in sorted_devs}

    def _cleanup_cold_blocks(self):
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
        if len(self.block_stats) <= max_entries:
            return
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
        if len(df.columns) >= 5:
            col_strs = [str(c).strip() for c in df.columns[:5]]
            if col_strs == ['0', '1', '2', '3', '4']:
                df.columns = list(IO_TRACE_COLUMNS) + list(df.columns[5:])
                return df[IO_TRACE_COLUMNS]
            df = df.iloc[:, :5].copy()
            df.columns = IO_TRACE_COLUMNS
        return df

    # ---- 序列化 / 反序列化 ----

    def to_checkpoint_state(self) -> dict:
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
        self.total_rows = state.get('total_rows_processed', 0)
        self.processed_members = list(state.get('processed_members', []))
        self.total_read = state.get('global', {}).get('total_read', 0)
        self.total_write = state.get('global', {}).get('total_write', 0)
        self.hour_global = defaultdict(int, state.get('global', {}).get('hour_global', {}))
        self.minute_global = defaultdict(int, state.get('global', {}).get('minute_global', {}))
        self.second_global = defaultdict(int, state.get('global', {}).get('second_global', {}))
        self.size_welford = state.get('global', {}).get('size_welford',
                                                        {'count': 0, 'mean': 0.0, 'M2': 0.0})

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
            self.ts_device[dev_id] = defaultdict(lambda: {
                'total_count': 0, 'read_count': 0, 'write_count': 0,
                'total_size_kb': 0.0, 'sum_size_kb': 0.0,
            })
            for wk_str, v in ts_data.items():
                wk = int(wk_str)
                self.ts_device[dev_id][wk] = dict(v)

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
        rows = []
        for dev_id, ds in self.dev_stats.items():
            total_count = ds['total_count']
            if total_count == 0:
                continue
            read_count = ds['read_count']
            active_windows = len(ds['window_counts'])
            window_sec = self.time_window_sec

            peak_iops = (max(ds['window_counts'].values()) / window_sec
                         if ds['window_counts'] else 0)
            peak_hour = max(ds['hour_counts'], key=ds['hour_counts'].get) if ds['hour_counts'] else 0

            avg_size = (ds['sum_size'] / total_count) / 1024.0 if total_count > 0 else 0
            std_size = np.sqrt(max(0, (ds['sum_size_sq'] / total_count) -
                                   (ds['sum_size'] / total_count) ** 2)) / 1024.0
            rows.append({
                'device_id': dev_id,
                'total_requests': total_count,
                'total_bytes': ds['total_bytes'],
                'read_requests': read_count,
                'write_requests': ds['write_count'],
                'read_ratio': read_count / total_count if total_count > 0 else 0,
                'avg_request_size_kb': avg_size,
                'std_request_size_kb': std_size,
                'first_active': (pd.Timestamp(ds['first_active_us'], unit='us', tz='Asia/Shanghai')
                                 if ds['first_active_us'] else None),
                'last_active': (pd.Timestamp(ds['last_active_us'], unit='us', tz='Asia/Shanghai')
                                if ds['last_active_us'] else None),
                'active_windows': active_windows,
                'peak_iops': peak_iops,
                'peak_hour': peak_hour,
            })

        df = pd.DataFrame(rows)
        if len(df) > 0:
            df = df.sort_values('total_requests', ascending=False).reset_index(drop=True)
        return df

    def get_time_series_global_df(self) -> pd.DataFrame:
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
        total = self.total_rows
        read_ratio = self.total_read / total if total > 0 else 0

        wf = self.size_welford
        if wf['count'] > 0:
            std = np.sqrt(wf['M2'] / wf['count']) if wf['count'] > 1 else 0.0
        else:
            std = 0.0

        seq_ratios = []
        for ds in self.dev_stats.values():
            if ds['total_pairs'] > 0:
                seq_ratios.append(ds['sequential_pairs'] / ds['total_pairs'])
        seq_ratio = np.mean(seq_ratios) if seq_ratios else 0
        seq_std = np.std(seq_ratios) if seq_ratios else 0

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

        peak_hour = max(self.hour_global, key=self.hour_global.get) if self.hour_global else None
        off_peak = min(self.hour_global, key=self.hour_global.get) if self.hour_global else None
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
