"""
流式Tar.gz处理器（支持断点续传）
================================

处理大规模 tar.gz 文件的流式方案：
1. HTTP Range 下载 + 边下边解压边处理
2. 只保留聚合分析结果，丢弃原始数据
3. checkpoints 支持中断后从上一次位置继续
4. 增量聚合：device profiles、时间序列、热点、访问模式

适用于 20-50GB 级别的阿里云 OSS trace 数据。

模块结构:
    checkpoint_manager.py   - CheckpointManager（原子写入 checkpoint）
    incremental_aggregator.py - IncrementalAggregator（增量聚合引擎）
    iter_reader.py          - IterReader（HTTP chunk → file-like 包装器）
    streaming_processor.py   - StreamingTarProcessor（流式编排器）
"""

import os
import io
import gzip
import json
import time
import tarfile
import threading
import shutil
import traceback
import logging
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from src.data_download.checkpoint_manager import CheckpointManager
from src.data_download.incremental_aggregator import IncrementalAggregator
from src.data_download.iter_reader import IterReader

logger = logging.getLogger(__name__)


# ================================================================
# 流式处理器
# ================================================================

class StreamingTarProcessor:
    """
    流式 tar.gz 处理器

    通过 HTTP 流式下载，边下载边解压边处理。
    支持中断后从 checkpoint 恢复。

    process() 内部完成 processing → incremental save → final save 全链路，
    调用方无需再单独调用 save_aggregation。
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
        3. 处理每个 csv 成员时增量聚合，定期保存 checkpoint
        4. max_gb/max_rows 达到限制时自动停止
        5. 完成后自动保存最终聚合结果到 data/device_analysis/

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

        # 2. 读取限制配置
        max_rows = self.aggregator.max_rows
        max_gb_total = self.config.get('max_gb', 0)
        if max_gb_total <= 0:
            max_gb_total = self.config.get('partial_end_gb', 0)
        max_total_bytes = max_gb_total * (1024 ** 3) if max_gb_total > 0 else 0

        # 3. 检查 checkpoint 并恢复聚合状态
        cp_state = self.checkpoint.load()
        if cp_state:
            self.bytes_downloaded = cp_state.get('total_bytes_downloaded', 0)
            self.decompressed_bytes = cp_state.get('decompressed_bytes', 0)
            self.aggregator.from_checkpoint_state(cp_state.get('aggregator', {}))
            msg = (f"从 checkpoint 恢复: 累计已解压 {self.decompressed_bytes / (1024**3):.2f} GB, "
                   f"已聚合 {self.aggregator.total_rows:,} 行, "
                   f"{len(self.aggregator.processed_members)} 个成员已处理")
            if max_gb_total > 0:
                remaining = max_gb_total - self.decompressed_bytes / (1024**3)
                msg += f" | 总量目标 {max_gb_total:.2f} GB，剩余 {remaining:.2f} GB"
                if remaining <= 0:
                    msg += " (已达总量上限)"
            logger.info(msg)
        else:
            self.bytes_downloaded = 0
            self.decompressed_bytes = 0
            self.aggregator.reset()
            msg = "从零开始 — 无 checkpoint"
            if max_gb_total > 0:
                msg += f"（总量目标 {max_gb_total:.2f} GB）"
            logger.info(msg)

        # 记录本次运行开始时的累计值
        self.decompressed_at_start = self.decompressed_bytes

        # 4. 检查是否已达上限
        if max_rows > 0 and self.aggregator.total_rows >= max_rows:
            logger.info(f"已达目标行数 {max_rows:,}，跳过处理")
            results = self._build_final_results()
            results['_stream_ended_naturally'] = False
            return results
        if max_total_bytes > 0 and self.decompressed_bytes >= max_total_bytes:
            logger.info(f"累计已达总量目标 {max_gb_total} GB，跳过处理")
            results = self._build_final_results()
            results['_stream_ended_naturally'] = False
            return results

        # 5. 获取远程文件大小
        self.total_file_size = self.get_file_size()
        if not self.total_file_size:
            logger.warning("无法获取文件大小")

        # 6. 真正的流式处理：HTTP → gzip → tar → 增量聚合
        self._true_stream_process(progress_callback, max_gb_total)

        # 7. 导出结果 → 最终保存到 data/device_analysis/
        results = self._build_final_results()
        results['_stream_ended_naturally'] = self._stream_ended_naturally
        logger.info(f"流式处理完成: {self.aggregator.total_rows:,} 行, "
                    f"{len(self.aggregator.dev_stats)} 个设备, "
                    f"{len(self.aggregator.block_stats)} 个热点块")

        # 8. 最终保存聚合数据（close the gap: 以前在 pipeline 里单独调用）
        if self.output_dir:
            try:
                self._incremental_save()
            except Exception as e:
                logger.warning(f"最终增量保存失败: {e}")

        return results

    def _true_stream_process(self, progress_callback=None, max_gb_total=0):
        """
        真正的流式处理管道：HTTP → gzip → tar → 逐成员聚合。
        不保存任何原始数据到磁盘。

        max_gb_total: 累计总量目标（跨运行累积，如 20GB），达到后自动停止
        断点续传策略：重新下载，跳过已处理的 tar 成员。
        """
        processed = set(self.aggregator.processed_members)
        max_total_bytes = max_gb_total * (1024 ** 3) if max_gb_total > 0 else 0
        rows_since_checkpoint = 0
        checkpoints_since_inc = 0
        inc_save_first_done = False
        member_count = 0
        decomp_at_start = self.decompressed_at_start
        self._stream_ended_naturally = False

        if processed:
            logger.info(f"断点续传：将重新下载数据，跳过 {len(processed)} 个已处理成员")

        # ---- 下载进度后台线程 ----
        download_reader = [None]

        def _progress_thread():
            sleep_sec = 5
            fast_count = 0
            last_mb = 0
            while download_reader[0] is not None:
                time.sleep(sleep_sec)
                rd = download_reader[0]
                if rd is None:
                    break
                mb_now = rd.bytes_total / (1024 ** 2)
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

                cl = response.headers.get('content-length')
                if cl:
                    self.total_file_size = max(int(cl), self.total_file_size or 0)

                response_iter = response.iter_content(chunk_size=65536)
                reader = IterReader(response_iter)
                download_reader[0] = reader

                info_parts = ["正在流式下载并解析数据"]
                if max_gb_total > 0:
                    info_parts.append(f"累计上限 {max_gb_total:.2f} GB")
                info_parts.append("（前25秒每5秒汇报，之后每30秒）")
                logger.info('，'.join(info_parts))

                with gzip.GzipFile(fileobj=reader) as gz:
                    with tarfile.open(fileobj=gz, mode='r|') as tar:
                        for member in tar:
                            member_count += 1

                            if member.name in processed:
                                logger.debug(f"跳过已处理成员 [{member_count}]: {member.name}")
                                continue

                            self.bytes_downloaded = reader.bytes_total

                            if member.isfile() and member.name.endswith('.csv') \
                                    and 'device_size' not in member.name.lower():
                                logger.info(f"处理 [{member_count}]: {member.name} "
                                            f"({member.size / (1024**2):.1f} MB)")
                                rows_before = self.aggregator.total_rows
                                should_continue, actual_bytes, rows_carry, ckpt_triggers = self._process_tar_member(
                                    tar, member, decomp_at_start,
                                    rows_carry_in=rows_since_checkpoint,
                                    checkpoint_interval=self.checkpoint_interval_rows,
                                    checkpoint_callback=self._save_checkpoint_light,
                                )
                                rows_processed_val = self.aggregator.total_rows - rows_before
                                self.decompressed_bytes += actual_bytes
                                self.aggregator.processed_members.append(member.name)

                                # 更新 rows_since_checkpoint（member 内可能已触发过保存）
                                rows_since_checkpoint = rows_carry
                                checkpoints_since_inc += ckpt_triggers

                                # 增量保存分析结果到 data/device_analysis/
                                # 第一个成员处理完立即保存；后续每 5 次 checkpoint（≈500 万行）
                                if self.output_dir:
                                    do_save = False
                                    if not inc_save_first_done:
                                        do_save = True
                                        inc_save_first_done = True
                                    elif checkpoints_since_inc >= 5:
                                        do_save = True
                                    if do_save:
                                        self._incremental_save()
                                        checkpoints_since_inc = 0

                                # 进度回调
                                if progress_callback:
                                    progress_callback(self.bytes_downloaded, max_total_bytes,
                                                      self.aggregator.total_rows, 'stream')

                                # 检查限制
                                max_rows_val = self.aggregator.max_rows
                                if max_rows_val > 0 and self.aggregator.total_rows >= max_rows_val:
                                    logger.info(f"已达目标行数 {max_rows_val:,}，停止处理")
                                    self._save_checkpoint_light()
                                    rows_since_checkpoint = 0
                                    return
                                if max_total_bytes > 0 and self.decompressed_bytes >= max_total_bytes:
                                    logger.info(f"累计已达总量目标 {max_gb_total:.2f} GB，停止处理")
                                    self._save_checkpoint_light()
                                    rows_since_checkpoint = 0
                                    return
                                if not should_continue:
                                    self._save_checkpoint_light()
                                    rows_since_checkpoint = 0
                                    return

                            elif member.isfile() and member.name.endswith('.json'):
                                logger.info(f"扫描 [{member_count}]: {member.name} (JSON, 跳过)")
                            elif member.isfile():
                                logger.info(f"扫描 [{member_count}]: {member.name} (非CSV, 跳过)")

                logger.info(f"数据流传输完成，共读取 {reader.bytes_total / (1024**3):.2f} GB, "
                            f"{member_count} 个 tar 成员")
                self._stream_ended_naturally = True
                break

            except Exception as e:
                logger.warning(f"流式处理失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
            finally:
                download_reader[0] = None
                if rows_since_checkpoint > 0:
                    self._save_checkpoint_light()

    def _process_tar_member(self, tar: tarfile.TarFile, member: tarfile.TarInfo,
                            decomp_at_start=0, rows_carry_in=0,
                            checkpoint_interval=0, checkpoint_callback=None):
        """处理单个 tar 成员中的 CSV 数据（支持 member 内部 checkpoint）。
        返回 (should_continue, bytes_read_total, rows_carry_out, ckpt_triggers)：
          - should_continue  True 继续处理下一个成员
          - bytes_read_total  实际读取的字节数
          - rows_carry_out    member 内未达到 checkpoint 阈值的行数（带回调用方）
          - ckpt_triggers     member 内部 checkpoint 回调触发次数
        """
        fileobj = tar.extractfile(member)
        if fileobj is None:
            return True, 0, rows_carry_in, 0

        try:
            pending = b''
            block_size = 20 * 1024 * 1024  # 20 MB
            bytes_processed = 0
            last_log_mb = 0
            last_log_time = time.time()
            rows_since_ckpt = rows_carry_in
            ckpt_count = 0

            while True:
                data = fileobj.read(block_size)
                if not data:
                    break

                bytes_processed += len(data)
                pending += data

                mb_processed = bytes_processed // (1024 * 1024)
                now = time.time()
                if mb_processed >= last_log_mb + 20 or (now - last_log_time >= 15 and mb_processed > last_log_mb):
                    last_log_mb = mb_processed
                    last_log_time = now
                    cum_now = (decomp_at_start + self.decompressed_bytes + bytes_processed) / (1024**3)
                    logger.info(f"  {member.name}: 已解压 {mb_processed} MB "
                                f"| 累计 {cum_now:.3f} GB | 已聚合 {self.aggregator.total_rows:,} 行")

                last_nl = pending.rfind(b'\n')
                if last_nl == -1:
                    continue

                complete = pending[:last_nl + 1]
                pending = pending[last_nl + 1:]

                buf = io.BytesIO(complete)
                rows_before_chunk = self.aggregator.total_rows
                for chunk in pd.read_csv(buf, chunksize=100000, header=None,
                                         low_memory=False):
                    if len(chunk) > 0:
                        should_continue = self.aggregator.ingest_chunk(chunk, member.name)
                        if not should_continue:
                            return False, bytes_processed, rows_since_ckpt, ckpt_count

                # 增量入 chunk 后的行数变化
                rows_added = self.aggregator.total_rows - rows_before_chunk
                rows_since_ckpt += rows_added

                # member 内部触发 checkpoint（每 checkpoint_interval 行）
                while checkpoint_callback and checkpoint_interval > 0 and rows_since_ckpt >= checkpoint_interval:
                    checkpoint_callback()
                    rows_since_ckpt -= checkpoint_interval
                    ckpt_count += 1

            if pending.strip():
                buf = io.BytesIO(pending)
                rows_before_chunk = self.aggregator.total_rows
                for chunk in pd.read_csv(buf, chunksize=100000, header=None,
                                         low_memory=False):
                    if len(chunk) > 0:
                        should_continue = self.aggregator.ingest_chunk(chunk, member.name)
                        if not should_continue:
                            return False, bytes_processed, rows_since_ckpt, ckpt_count

                rows_added = self.aggregator.total_rows - rows_before_chunk
                rows_since_ckpt += rows_added
                while checkpoint_callback and checkpoint_interval > 0 and rows_since_ckpt >= checkpoint_interval:
                    checkpoint_callback()
                    rows_since_ckpt -= checkpoint_interval
                    ckpt_count += 1

            return True, bytes_processed, rows_since_ckpt, ckpt_count
        except Exception as e:
            logger.warning(f"处理 {member.name} 时出错: {e}")
            logger.debug(traceback.format_exc())
            return True, 0

    def _incremental_save(self):
        """增量保存当前分析结果到 output_dir（用户可见的 CSV/JSON）。
        checkpoint 已由 _save_checkpoint_light 每 100 万行独立保存，此处不再重复。"""
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
            # 裁剪 block_stats 释放内存（保留 Top 30000 热点 block）
            self.aggregator.prune_block_stats(max_entries=30000)
        except Exception as e:
            logger.warning(f"增量保存失败: {e}", exc_info=True)

    def _save_checkpoint_light(self):
        """保存 checkpoint（轻量版：block 裁剪到 2000，适合频繁保存）。

        带错误处理：序列化/json 写入失败不会中断流式处理，仅记录警告。
        """
        try:
            self.aggregator.prune_block_stats(max_entries=2000)
            state = {
                'total_bytes_downloaded': self.bytes_downloaded,
                'decompressed_bytes': self.decompressed_bytes,
                'total_file_size': self.total_file_size,
                'aggregator': self.aggregator.to_checkpoint_state(),
            }
            logger.info(f"正在保存 checkpoint ({self.aggregator.total_rows:,} 行, "
                        f"{len(self.aggregator.dev_stats):,} 设备, "
                        f"{len(self.aggregator.block_stats):,} blocks)...")
            self.checkpoint.save(state)
            logger.info(f"checkpoint 已保存到 {self.checkpoint.checkpoint_file}")
        except Exception as e:
            logger.warning(f"_save_checkpoint_light 失败（处理将继续）: {e}", exc_info=True)

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
        if self.temp_file.exists():
            try:
                self.temp_file.unlink()
                logger.info(f"已清理临时文件: {self.temp_file}")
            except Exception as e:
                logger.warning(f"清理临时文件失败: {e}")

        self.checkpoint.clear()
        self._cleanup_work_dir()
        self._cleanup_empty_data_dirs()

    def _cleanup_work_dir(self):
        """清理 checkpoint 工作目录（如已空则删除）"""
        work_dir = self.work_dir
        if not work_dir.exists():
            return
        try:
            if self.checkpoint.exists():
                self.checkpoint.clear()
            remaining = list(work_dir.iterdir())
            if not remaining:
                work_dir.rmdir()
                logger.info(f"已清理工作目录: {work_dir}")
        except Exception as e:
            logger.debug(f"清理工作目录时忽略: {e}")

    @staticmethod
    def _cleanup_empty_data_dirs():
        """清理 data/ 下空的子目录（processed, raw 等）"""
        data_dir = Path('data')
        if not data_dir.exists():
            return
        for sub in list(data_dir.iterdir()):
            if sub.is_dir():
                try:
                    remaining = list(sub.iterdir())
                    if not remaining:
                        if sub.name == 'device_analysis':
                            continue
                        sub.rmdir()
                        logger.info(f"已清理空目录: {sub}")
                except Exception:
                    pass

    def save_aggregation(self, save_dir: str, keep_checkpoint: bool = False) -> dict:
        """
        阶段1: 仅保存聚合数据（CSV + JSON），不生成可视化和报告。

        Args:
            save_dir: 保存目录
            keep_checkpoint: 是否保留 checkpoint（部分完成时保留以便续传）

        Returns:
            分析结果字典
        """
        from src.device_load_analysis.load_reporter import LoadReporter

        results = self._build_final_results()
        reporter = LoadReporter(save_dir=save_dir)

        profiles = results['device_profiles']
        patterns = results['access_patterns']
        ts_global = results['time_series_global']

        reporter.print_summary(profiles, patterns, ts_global)
        reporter.save_results(results, save_dir)

        if not keep_checkpoint:
            ckpt_file = self.checkpoint.checkpoint_file
            was_present = ckpt_file.exists()
            old_size = ckpt_file.stat().st_size if was_present else 0
            self.checkpoint.clear()
            self._cleanup_work_dir()
            self._cleanup_empty_data_dirs()
            if was_present:
                logger.info(f"数据流完整结束 → checkpoint 已清除 (原 {old_size:,} bytes)")
            else:
                logger.info("数据流完整结束，无 checkpoint 需清理")
        else:
            ckpt_file = self.checkpoint.checkpoint_file
            logger.info(f"checkpoint 已保留（{ckpt_file}），可用 --resume 继续处理")

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

        results = self._load_aggregation_from_dir(data_dir)
        if results is None:
            logger.error("无法加载聚合数据，请先运行阶段1")
            return None

        reporter = LoadReporter(save_dir=viz_dir)
        profiles = results['device_profiles']
        patterns = results['access_patterns']
        ts_global = results['time_series_global']

        reporter.print_summary(profiles, patterns, ts_global)
        reporter.visualize_load(results, viz_dir)
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

            if 'device_profiles' not in results and 'time_series_global' not in results:
                logger.warning("聚合目录中数据不完整（缺少 device_profiles 和 time_series_global）")
                return None

            total = results.get('total_rows_processed',
                                len(results.get('time_series_global', pd.DataFrame())))
            logger.info(f"已加载聚合数据: {save_dir} ({total:,} 行)")

        except Exception as e:
            logger.error(f"加载聚合数据失败: {e}")
            return None

        return results
