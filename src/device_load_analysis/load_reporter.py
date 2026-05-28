"""
设备负载报告与可视化模块
负责分析结果的摘要打印、结果保存和负载可视化图表生成
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os
import json
import logging

logger = logging.getLogger(__name__)


def _setup_chinese_font():
    """设置中文字体"""
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False


class LoadReporter:
    """设备负载报告生成器"""

    def __init__(self, save_dir=None):
        self.save_dir = save_dir
        self.figsize = (14, 7)

    def print_summary(self, profiles, patterns, ts_global):
        """打印分析摘要到控制台"""
        print("\n" + "=" * 60)
        print("设备负载分析摘要")
        print("=" * 60)

        print(f"\n[数据概况]")
        print(f"  设备总数: {len(profiles)}")
        print(f"  总请求数: {profiles['total_requests'].sum():,}")

        print(f"\n[前5高负载设备]")
        top5 = profiles.head(5)[['device_id', 'total_requests', 'read_ratio',
                                  'avg_request_size_kb', 'peak_iops', 'peak_hour']]
        print(top5.to_string(index=False))

        print(f"\n[访问模式]")
        print(f"  顺序访问比例: {patterns.get('sequential_access_ratio', 0):.1%}")
        print(f"  读操作占比: {patterns.get('global_read_ratio', 0):.1%}")
        print(f"  主要请求大小: {patterns.get('dominant_request_size', 'N/A')}")
        print(f"  峰值小时: {patterns.get('peak_hour', 'N/A')}:00")
        print(f"  低谷小时: {patterns.get('off_peak_hour', 'N/A')}:00")
        print(f"  活跃窗口占比: {patterns.get('active_window_ratio', 0):.1%}")

        if len(ts_global) > 0:
            print(f"\n[全局负载统计]")
            print(f"  时间窗口数: {len(ts_global)}")
            print(f"  平均IOPS: {ts_global['iops'].mean():.1f}")
            print(f"  峰值IOPS: {ts_global['iops'].max():.1f}")
            print(f"  平均吞吐: {ts_global['throughput_kb'].mean():.1f} KB/s")
            print(f"  峰值吞吐: {ts_global['throughput_kb'].max():.1f} KB/s")
            if len(ts_global) <= 3:
                print(f"  [注意] 时间窗口数较少，趋势图可能不够丰富，建议增加数据量")

        print("=" * 60)

    def save_results(self, analysis_results, save_dir=None):
        """保存分析结果到文件（原子性写入：先写临时文件再 rename）"""
        save_dir = save_dir or self.save_dir
        if not save_dir:
            logger.warning("未指定保存目录，跳过保存")
            return
        os.makedirs(save_dir, exist_ok=True)

        saved_count = 0
        failed_items = []

        for key, data in analysis_results.items():
            try:
                if isinstance(data, pd.DataFrame):
                    filepath = os.path.join(save_dir, f'{key}.csv')
                    # 原子性写入：先写临时文件再 rename
                    tmp_path = filepath + '.tmp'
                    data.to_csv(tmp_path, index=False)
                    os.replace(tmp_path, filepath)
                    logger.info(f"保存: {filepath} ({len(data)} 行)")
                    saved_count += 1
                elif isinstance(data, dict):
                    filepath = os.path.join(save_dir, f'{key}.json')
                    serializable = {}
                    for k, v in data.items():
                        if isinstance(v, (np.integer,)):
                            serializable[k] = int(v)
                        elif isinstance(v, (np.floating,)):
                            serializable[k] = float(v)
                        elif isinstance(v, np.ndarray):
                            serializable[k] = v.tolist()
                        elif isinstance(v, dict):
                            serializable[k] = {str(kk): (int(vv) if isinstance(vv, (np.integer,)) else vv)
                                               for kk, vv in v.items()}
                        else:
                            serializable[k] = v
                    # 原子性写入：先写临时文件再 rename
                    tmp_path = filepath + '.tmp'
                    with open(tmp_path, 'w', encoding='utf-8') as f:
                        json.dump(serializable, f, ensure_ascii=False, indent=2)
                    os.replace(tmp_path, filepath)
                    logger.info(f"保存: {filepath}")
                    saved_count += 1
            except Exception as e:
                logger.error(f"保存 {key} 失败: {e}")
                failed_items.append(key)
                # 清理可能残留的临时文件
                tmp_path = os.path.join(save_dir, f'{key}.csv.tmp')
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                tmp_path = os.path.join(save_dir, f'{key}.json.tmp')
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        if failed_items:
            logger.warning(f"保存完成: {saved_count}/{saved_count + len(failed_items)} 成功, "
                          f"失败项: {failed_items}")
        else:
            logger.info(f"全部 {saved_count} 项保存成功")

    # ================================================================
    # 服务器负载可视化
    # ================================================================

    def visualize_load(self, analysis_results, save_dir=None):
        """
        生成服务器负载可视化图表集

        需要分析结果中包含:
        - time_series_global: 全局时间序列负载数据
        - time_series_device: 按设备时间序列负载数据
        - device_profiles: 设备画像
        - access_patterns: 访问模式
        - hotspots: 热点块
        """
        _setup_chinese_font()
        save_dir = save_dir or self.save_dir
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            os.makedirs(os.path.join(save_dir, 'load_charts'), exist_ok=True)

        ts_global = analysis_results.get('time_series_global')
        ts_device = analysis_results.get('time_series_device')
        profiles = analysis_results.get('device_profiles')
        patterns = analysis_results.get('access_patterns')
        hotspots = analysis_results.get('hotspots')

        if ts_global is not None and len(ts_global) > 0:
            self._plot_iops_trend(ts_global, save_dir)
            self._plot_throughput_trend(ts_global, save_dir)
            self._plot_read_write_ratio(ts_global, save_dir)
        else:
            logger.warning("跳过全局趋势图: time_series_global 为空或数据不足")

        if ts_device is not None and len(ts_device) > 0:
            self._plot_top_devices_load(ts_device, save_dir)
        else:
            logger.warning("跳过设备对比图: time_series_device 为空")

        if profiles is not None and len(profiles) > 0:
            self._plot_device_ranking(profiles, save_dir)
            self._plot_request_size_distribution(profiles, save_dir)

        if patterns:
            self._plot_hourly_load(patterns, save_dir)
            self._plot_second_trend(patterns, save_dir)
            self._plot_access_pattern_overview(patterns, save_dir)

        if hotspots is not None and len(hotspots) > 0:
            self._plot_hotspot_distribution(hotspots, save_dir)
        else:
            logger.info("跳过热点块图: 无满足阈值的热点块")

        logger.info(f"负载可视化图表已生成，保存到: {save_dir}/load_charts/")

    def _save_fig(self, save_dir, filename):
        """保存当前图表"""
        if save_dir:
            path = os.path.join(save_dir, 'load_charts', filename)
            plt.savefig(path, dpi=150, bbox_inches='tight')
            logger.info(f"  图表保存: {path}")
        plt.close()

    def _plot_iops_trend(self, ts, save_dir):
        """全局 IOPS 时间趋势图（自适应数据量）"""
        fig, ax = plt.subplots(figsize=self.figsize)

        if len(ts) == 1:
            # 单点数据 → 柱状图
            x = [ts['datetime'].iloc[0]]
            ax.bar(x, ts['iops'], width=0.05, color='#1f77b4', alpha=0.8, label='Total IOPS')
            ax.bar(x, ts['iops_read'], width=0.05, color='#2ca02c', alpha=0.7, label='Read IOPS')
            ax.bar(x, ts['iops_write'], width=0.05, bottom=ts['iops_read'].values,
                   color='#d62728', alpha=0.7, label='Write IOPS')
            ax.set_title(f'Global IOPS (Single Window: {x[0]})', fontsize=16)
            ax.set_ylabel('IOPS', fontsize=12)
            # 在柱子上标注数值
            for val, label, color, yoff in [
                (ts['iops'].values[0], f"Total: {ts['iops'].values[0]:.0f}", '#1f77b4', 0.02),
                (ts['iops_read'].values[0], f"Read: {ts['iops_read'].values[0]:.0f}", '#2ca02c', 0.01),
                (ts['iops_write'].values[0], f"Write: {ts['iops_write'].values[0]:.0f}", '#d62728', 0.01),
            ]:
                ax.text(x[0], val + (ts['iops'].values[0] * yoff), label,
                        ha='center', fontsize=10, color=color, fontweight='bold')
        else:
            ax.plot(ts['datetime'], ts['iops'], label='Total IOPS', color='#1f77b4',
                    linewidth=0.8, alpha=0.9)
            ax.plot(ts['datetime'], ts['iops_read'], label='Read IOPS', color='#2ca02c',
                    linewidth=0.6, alpha=0.7)
            ax.plot(ts['datetime'], ts['iops_write'], label='Write IOPS', color='#d62728',
                    linewidth=0.6, alpha=0.7)
            ax.fill_between(ts['datetime'], 0, ts['iops'], alpha=0.1, color='#1f77b4')
            ax.set_title(f'Global IOPS Trend ({len(ts)} windows)', fontsize=16)
            ax.set_xlabel('Time', fontsize=12)
            ax.set_ylabel('IOPS', fontsize=12)

        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        self._save_fig(save_dir, 'iops_trend.png')

    def _plot_throughput_trend(self, ts, save_dir):
        """全局吞吐量时间趋势图（自适应数据量）"""
        fig, ax = plt.subplots(figsize=self.figsize)

        if len(ts) == 1:
            x = [ts['datetime'].iloc[0]]
            read_tp = (ts['throughput_kb'] * ts['read_ratio'].fillna(0)).values[0]
            write_tp = (ts['throughput_kb'] * (1 - ts['read_ratio'].fillna(0))).values[0]
            total_tp = ts['throughput_kb'].values[0]
            ax.bar(x, [read_tp], width=0.05, color='#2ca02c', alpha=0.7, label='Read')
            ax.bar(x, [write_tp], width=0.05, bottom=[read_tp], color='#d62728',
                   alpha=0.7, label='Write')
            ax.set_title(f'Global Throughput (Single Window: {x[0]})', fontsize=16)
            ax.set_ylabel('Throughput (KB/s)', fontsize=12)
            ax.text(x[0], total_tp * 1.05, f"Total: {total_tp:.0f} KB/s",
                    ha='center', fontsize=10, fontweight='bold')
        else:
            ax.plot(ts['datetime'], ts['throughput_kb'], label='Total Throughput',
                    color='#1f77b4', linewidth=0.8, alpha=0.9)
            ax.fill_between(ts['datetime'], 0, ts['throughput_kb'], alpha=0.1, color='#1f77b4')
            ax.set_title(f'Global Throughput Trend ({len(ts)} windows)', fontsize=16)
            ax.set_xlabel('Time', fontsize=12)
            ax.set_ylabel('Throughput (KB/s)', fontsize=12)

        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        self._save_fig(save_dir, 'throughput_trend.png')

    def _plot_read_write_ratio(self, ts, save_dir):
        """读写比例时间趋势图（自适应数据量）"""
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))

        # 左图：读写比例趋势（或单点柱状）
        if len(ts) == 1:
            read_pct = ts['read_ratio'].fillna(0).values[0] * 100
            axes[0].bar(['R/W'], [read_pct], color='#2ca02c', alpha=0.8, label='Read')
            axes[0].bar(['R/W'], [100 - read_pct], bottom=[read_pct], color='#d62728',
                        alpha=0.8, label='Write')
            axes[0].set_title(f'Read/Write Ratio (Single Window)', fontsize=14)
            axes[0].set_ylabel('Ratio (%)', fontsize=12)
            axes[0].set_ylim(0, 100)
            axes[0].text('R/W', read_pct / 2, f'{read_pct:.1f}%', ha='center', va='center',
                         fontsize=12, fontweight='bold', color='white')
            axes[0].text('R/W', read_pct + (100 - read_pct) / 2,
                         f'{100 - read_pct:.1f}%', ha='center', va='center',
                         fontsize=12, fontweight='bold', color='white')
        else:
            axes[0].fill_between(ts['datetime'], ts['read_ratio'].fillna(0) * 100,
                                 alpha=0.4, color='#2ca02c', label='Read %')
            axes[0].fill_between(ts['datetime'], ts['read_ratio'].fillna(0) * 100, 100,
                                 alpha=0.4, color='#d62728', label='Write %')
            axes[0].set_title('Read/Write Ratio Over Time', fontsize=14)
            axes[0].set_xlabel('Time', fontsize=12)
            axes[0].set_ylabel('Ratio (%)', fontsize=12)
            axes[0].set_ylim(0, 100)

        axes[0].legend(fontsize=11)
        axes[0].grid(True, alpha=0.3)

        # 右图：读写请求总数对比（饼图）
        total_read = int(ts['read_ops'].sum())
        total_write = int(ts['write_ops'].sum())
        if total_read + total_write > 0:
            sizes = [total_read, total_write]
            labels = [f'Read\n{total_read:,}', f'Write\n{total_write:,}']
            colors = ['#2ca02c', '#d62728']
            wedges, texts, autotexts = axes[1].pie(
                sizes, labels=labels, colors=colors, autopct='%1.1f%%',
                startangle=90, textprops={'fontsize': 11}
            )
            for at in autotexts:
                at.set_fontweight('bold')
            axes[1].set_title('Read vs Write Total Requests', fontsize=14)

        plt.tight_layout()
        self._save_fig(save_dir, 'read_write_ratio.png')

    def _plot_top_devices_load(self, ts_device, save_dir, top_n=5):
        """Top N 设备 IOPS 对比（自适应数据量）"""
        top_devices = ts_device.groupby('device_id')['iops'].mean().nlargest(top_n).index
        fig, axes = plt.subplots(1, 2, figsize=(16, 7))

        # 左图：平均IOPS柱状图（始终有意义）
        avg_iops = ts_device[ts_device['device_id'].isin(top_devices)].groupby('device_id')['iops'].mean()
        avg_iops = avg_iops.reindex(top_devices)
        colors = plt.cm.Set2(np.linspace(0, 1, top_n))
        axes[0].barh([f'D{d}' for d in avg_iops.index], avg_iops.values, color=colors, alpha=0.8)
        axes[0].invert_yaxis()
        axes[0].set_xlabel('Avg IOPS', fontsize=12)
        axes[0].set_title(f'Top {top_n} Devices - Avg IOPS', fontsize=14)
        for i, (val, dev) in enumerate(zip(avg_iops.values, avg_iops.index)):
            axes[0].text(val * 1.02, i, f'{val:.1f}', va='center', fontsize=9)
        axes[0].grid(True, axis='x', alpha=0.3)

        # 右图：时间序列（仅当时间窗口 > 1 时）
        if ts_device['datetime'].nunique() > 1:
            for device_id in top_devices:
                device_ts = ts_device[ts_device['device_id'] == device_id]
                axes[1].plot(device_ts['datetime'], device_ts['iops'], linewidth=0.6,
                             label=f'Device {device_id}')
            axes[1].set_title(f'Top {top_n} Devices IOPS Over Time', fontsize=14)
            axes[1].set_xlabel('Time', fontsize=12)
            axes[1].set_ylabel('IOPS', fontsize=12)
            axes[1].legend(fontsize=9, ncol=2)
        else:
            # 单时间窗口：展示读写拆分
            for idx, device_id in enumerate(top_devices):
                d_ts = ts_device[ts_device['device_id'] == device_id]
                if len(d_ts) > 0:
                    x = idx
                    axes[1].bar(x, d_ts['iops_read'].values[0], color='#2ca02c', alpha=0.7,
                                label='Read' if idx == 0 else '')
                    axes[1].bar(x, d_ts['iops_write'].values[0],
                                bottom=d_ts['iops_read'].values[0],
                                color='#d62728', alpha=0.7, label='Write' if idx == 0 else '')
            axes[1].set_xticks(range(len(top_devices)))
            axes[1].set_xticklabels([f'D{d}' for d in top_devices], fontsize=9)
            axes[1].set_title(f'Top {top_n} Devices Read/Write IOPS', fontsize=14)
            axes[1].set_ylabel('IOPS', fontsize=12)
            axes[1].legend(fontsize=9)

        axes[1].grid(True, alpha=0.3)
        plt.tight_layout()
        self._save_fig(save_dir, 'top_devices_iops.png')

    def _plot_device_ranking(self, profiles, save_dir, top_n=15):
        """设备负载排名"""
        top = profiles.head(top_n)
        fig, axes = plt.subplots(1, 2, figsize=(16, 7))

        # 按请求数排名
        axes[0].barh(range(len(top)), top['total_requests'], color='#1f77b4', alpha=0.8)
        axes[0].set_yticks(range(len(top)))
        axes[0].set_yticklabels([f'D{d}' for d in top['device_id']], fontsize=9)
        axes[0].invert_yaxis()
        axes[0].set_xlabel('Total Requests')
        axes[0].set_title(f'Top {top_n} Devices by Requests', fontsize=14)
        axes[0].grid(True, axis='x', alpha=0.3)

        # 按峰值 IOPS 排名
        axes[1].barh(range(len(top)), top['peak_iops'], color='#ff7f0e', alpha=0.8)
        axes[1].set_yticks(range(len(top)))
        axes[1].set_yticklabels([f'D{d}' for d in top['device_id']], fontsize=9)
        axes[1].invert_yaxis()
        axes[1].set_xlabel('Peak IOPS')
        axes[1].set_title(f'Top {top_n} Devices by Peak IOPS', fontsize=14)
        axes[1].grid(True, axis='x', alpha=0.3)

        plt.tight_layout()
        self._save_fig(save_dir, 'device_ranking.png')

    def _plot_request_size_distribution(self, profiles, save_dir):
        """请求大小分布"""
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))

        # 左图：直方图
        vals = profiles['avg_request_size_kb'].dropna()
        if len(vals) > 1:
            axes[0].hist(vals, bins=min(30, len(vals) // 2 + 1), color='#1f77b4',
                         alpha=0.7, edgecolor='white')
        else:
            axes[0].bar(['All Devices'], vals.values, color='#1f77b4', alpha=0.7)
        axes[0].set_title('Avg Request Size Distribution (per Device)', fontsize=14)
        axes[0].set_xlabel('Avg Request Size (KB)', fontsize=12)
        axes[0].set_ylabel('Device Count', fontsize=12)
        axes[0].grid(True, alpha=0.3)

        # 右图：Top 设备读写请求大小对比
        top = profiles.head(10)
        x = range(len(top))
        axes[1].bar(x, top['read_requests'], color='#2ca02c', alpha=0.7, label='Read Req')
        axes[1].bar(x, top['write_requests'], bottom=top['read_requests'],
                    color='#d62728', alpha=0.7, label='Write Req')
        axes[1].set_xticks(x)
        axes[1].set_xticklabels([f'D{d}' for d in top['device_id']], fontsize=9, rotation=45)
        axes[1].set_title('Top 10 Devices Read/Write Request Count', fontsize=14)
        axes[1].set_ylabel('Request Count', fontsize=12)
        axes[1].legend(fontsize=10)
        axes[1].grid(True, axis='y', alpha=0.3)

        plt.tight_layout()
        self._save_fig(save_dir, 'request_size_distribution.png')

    def _plot_hourly_load(self, patterns, save_dir):
        """按分钟粒度的负载分布（适合测试阶段短时间跨度数据）"""
        minute_dist = patterns.get('minute_distribution', {})
        hourly_dist = patterns.get('hourly_distribution', {})

        fig, axes = plt.subplots(1, 2, figsize=(18, 7))

        if minute_dist:
            # 按时间排序
            minutes = sorted(minute_dist.keys())
            values = [minute_dist[m] for m in minutes]

            # 左图：时间线柱状图
            bar_colors = plt.cm.Blues(np.linspace(0.3, 0.9, len(values)))
            axes[0].bar(range(len(values)), values, color=bar_colors, alpha=0.8, edgecolor='white')
            axes[0].set_xticks(range(0, len(values), max(1, len(values) // 20)))
            axes[0].set_xticklabels([minutes[i] for i in range(0, len(values), max(1, len(values) // 20))],
                                    fontsize=8, rotation=45)
            axes[0].set_title(f'Load by Minute ({len(values)} active minutes)', fontsize=14)
            axes[0].set_xlabel('Time (HH:MM)', fontsize=12)
            axes[0].set_ylabel('Request Count', fontsize=12)
            axes[0].grid(True, axis='y', alpha=0.3)

            # 标注峰值分钟
            peak_idx = int(np.argmax(values))
            axes[0].annotate(f'Peak: {values[peak_idx]:,}',
                             xy=(peak_idx, values[peak_idx]),
                             xytext=(peak_idx + len(values) * 0.05, values[peak_idx] * 0.9),
                             fontsize=9, color='#d62728', fontweight='bold',
                             arrowprops=dict(arrowstyle='->', color='#d62728', lw=1.2))

            # 右图：Top 10 活跃分钟饼图
            top_n_min = min(10, len(values))
            if top_n_min > 0:
                sorted_pairs = sorted(minute_dist.items(), key=lambda x: x[1], reverse=True)[:top_n_min]
                # "Other" 汇总
                other_sum = sum(v for _, v in minute_dist.items()) - sum(v for _, v in sorted_pairs)
                if other_sum > 0:
                    sorted_pairs.append(('Other', other_sum))

                labels = [f'{m}\n({v:,})' for m, v in sorted_pairs]
                sizes = [v for _, v in sorted_pairs]
                pie_colors = plt.cm.Blues(np.linspace(0.3, 0.9, len(sizes)))
                wedges, texts, autotexts = axes[1].pie(
                    sizes, labels=labels, colors=pie_colors, autopct='%1.1f%%',
                    startangle=90, textprops={'fontsize': 8}
                )
                for at in autotexts:
                    at.set_fontsize(7)
                    at.set_fontweight('bold')
                axes[1].set_title(f'Top {top_n_min} Active Minutes', fontsize=14)
        else:
            # 回退到按小时（如果分钟分布为空）
            if hourly_dist:
                hours = sorted(hourly_dist.keys(), key=int)
                values = [hourly_dist[h] for h in hours]
                axes[0].bar([int(h) for h in hours], values, color='#1f77b4', alpha=0.8)
                axes[0].set_title('Load by Hour (fallback)', fontsize=14)
                axes[0].set_xlabel('Hour')
                axes[0].set_ylabel('Request Count')
            else:
                axes[0].text(0.5, 0.5, 'No time distribution data', transform=axes[0].transAxes,
                             ha='center', va='center', fontsize=14, color='#888')
            axes[1].text(0.5, 0.5, 'No data', transform=axes[1].transAxes,
                         ha='center', va='center', fontsize=14, color='#888')
            fig.suptitle('Load Distribution (insufficient data span)', fontsize=16,
                         color='#888', style='italic')

        plt.tight_layout()
        self._save_fig(save_dir, 'minute_load.png')

    def _plot_second_trend(self, patterns, save_dir):
        """按秒粒度的 I/O 请求折线图（适合测试阶段短时间跨度数据）"""
        second_dist = patterns.get('second_distribution', {})
        if not second_dist:
            logger.info("跳过按秒折线图: second_distribution 为空")
            return

        seconds = sorted(second_dist.keys())
        values = [second_dist[s] for s in seconds]

        fig, ax = plt.subplots(figsize=(16, 6))

        if len(seconds) <= 1:
            ax.bar(seconds, values, color='#1f77b4', alpha=0.8, width=0.8)
            ax.set_title('I/O Requests by Second (Single Data Point)', fontsize=14)
            if values:
                ax.text(seconds[0], values[0] * 1.05, f'{values[0]:,}', ha='center',
                        fontsize=12, fontweight='bold', color='#1f77b4')
        else:
            # 折线图 + 填充区域
            ax.plot(range(len(values)), values, color='#1f77b4', linewidth=1.0, alpha=0.9)
            ax.fill_between(range(len(values)), 0, values, alpha=0.15, color='#1f77b4')

            # 峰值标注
            peak_idx = int(np.argmax(values))
            ax.annotate(f'Peak: {values[peak_idx]:,}',
                        xy=(peak_idx, values[peak_idx]),
                        xytext=(peak_idx + len(values) * 0.05, values[peak_idx] * 0.85),
                        fontsize=10, color='#d62728', fontweight='bold',
                        arrowprops=dict(arrowstyle='->', color='#d62728', lw=1.2))

            # 均值线
            mean_val = np.mean(values)
            ax.axhline(y=mean_val, color='#ff7f0e', linestyle='--', linewidth=1, alpha=0.7,
                       label=f'Mean: {mean_val:.0f}')

            # x轴刻度：最多显示20个刻度标签
            step = max(1, len(seconds) // 20)
            ax.set_xticks(range(0, len(seconds), step))
            ax.set_xticklabels([seconds[i] for i in range(0, len(seconds), step)],
                               fontsize=8, rotation=45)

            ax.set_title(f'I/O Requests per Second ({len(seconds)} seconds, Total: {sum(values):,})',
                         fontsize=14)

        ax.set_xlabel('Time (HH:MM:SS)', fontsize=12)
        ax.set_ylabel('Request Count', fontsize=12)
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        self._save_fig(save_dir, 'second_trend.png')

    def _plot_access_pattern_overview(self, patterns, save_dir):
        """访问模式概览（仪表盘风格，不依赖时间跨度）"""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # 1. 读写比例饼图
        read_r = patterns.get('global_read_ratio', 0)
        sizes = [read_r, 1 - read_r]
        axes[0, 0].pie(sizes, labels=['Read', 'Write'], colors=['#2ca02c', '#d62728'],
                       autopct='%1.1f%%', startangle=90, textprops={'fontsize': 12})
        axes[0, 0].set_title('Read/Write Ratio', fontsize=14, fontweight='bold')

        # 2. 顺序度仪表
        seq_ratio = patterns.get('sequential_access_ratio', 0)
        seq_std = patterns.get('sequential_access_std', 0)
        categories = ['Sequential', 'Random']
        values = [seq_ratio, 1 - seq_ratio]
        axes[0, 1].barh(categories, values, color=['#1f77b4', '#ff7f0e'], alpha=0.8)
        for i, v in enumerate(values):
            axes[0, 1].text(v + 0.02, i, f'{v:.1%}', va='center', fontsize=11, fontweight='bold')
        axes[0, 1].set_xlim(0, 1.1)
        axes[0, 1].set_title(f'Access Pattern (σ={seq_std:.2f})', fontsize=14, fontweight='bold')
        axes[0, 1].grid(True, axis='x', alpha=0.3)

        # 3. 请求大小分布
        size_stats = patterns.get('request_size_stats', {})
        if size_stats:
            quartiles = ['Min', '25%', 'Median', '75%', 'Max']
            q_vals = [size_stats.get('min', 0), size_stats.get('25%', 0),
                      size_stats.get('50%', 0), size_stats.get('75%', 0), size_stats.get('max', 0)]
            q_kb = [v / 1024 for v in q_vals]
            axes[1, 0].bar(quartiles, q_kb, color='#9467bd', alpha=0.8)
            for i, v in enumerate(q_kb):
                axes[1, 0].text(i, v + max(q_kb) * 0.02, f'{v:.1f}', ha='center', fontsize=9)
            axes[1, 0].set_title('Request Size Distribution (KB)', fontsize=14, fontweight='bold')
            axes[1, 0].set_ylabel('KB', fontsize=12)
            axes[1, 0].grid(True, axis='y', alpha=0.3)

            dominant = patterns.get('dominant_request_size', 0)
            axes[1, 0].text(0.98, 0.95, f'Dominant size: {dominant} B',
                            transform=axes[1, 0].transAxes, ha='right', va='top',
                            fontsize=10, color='#555',
                            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

        # 4. 关键指标数字面板
        axes[1, 1].axis('off')
        metrics = [
            ('Total Requests', f"{patterns.get('request_size_stats', {}).get('count', 0):,.0f}"),
            ('Sequential Ratio', f"{seq_ratio:.1%}"),
            ('Read Ratio', f"{read_r:.1%}"),
            ('Peak Hour', f"{patterns.get('peak_hour', 'N/A')}:00"),
            ('Off-Peak Hour', f"{patterns.get('off_peak_hour', 'N/A')}:00"),
            ('Active Window Ratio', f"{patterns.get('active_window_ratio', 0):.1%}"),
        ]
        for i, (label, value) in enumerate(metrics):
            y = 0.92 - i * 0.14
            axes[1, 1].text(0.05, y, label, transform=axes[1, 1].transAxes,
                            fontsize=12, color='#666')
            axes[1, 1].text(0.95, y, value, transform=axes[1, 1].transAxes,
                            fontsize=14, fontweight='bold', ha='right', color='#1a1a2e')
        axes[1, 1].set_title('Key Metrics', fontsize=14, fontweight='bold')

        plt.suptitle('I/O Access Pattern Overview', fontsize=18, fontweight='bold', y=1.02)
        plt.tight_layout()
        self._save_fig(save_dir, 'access_pattern_overview.png')

    def _plot_hotspot_distribution(self, hotspots, save_dir):
        """热点块访问频率分布"""
        fig, axes = plt.subplots(1, 2, figsize=(16, 7))

        top20 = hotspots.head(20)

        # 热点块访问次数
        axes[0].barh(range(len(top20)), top20['access_count'].values,
                     color='#d62728', alpha=0.8)
        axes[0].set_yticks(range(len(top20)))
        axes[0].set_yticklabels(
            [f'D{r["device_id"]}-B{r["block_id"]}' for _, r in top20.iterrows()],
            fontsize=8
        )
        axes[0].invert_yaxis()
        axes[0].set_xlabel('Access Count')
        axes[0].set_title('Top Hot Blocks by Access Frequency', fontsize=14)
        axes[0].grid(True, axis='x', alpha=0.3)

        # 读写分布
        x = range(len(top20))
        axes[1].bar(x, top20['read_count'], color='#2ca02c', alpha=0.8, label='Read')
        axes[1].bar(x, top20['write_count'], bottom=top20['read_count'],
                    color='#d62728', alpha=0.8, label='Write')
        axes[1].set_xticks(x)
        axes[1].set_xticklabels(
            [f'D{r["device_id"]}' for _, r in top20.iterrows()],
            rotation=45, fontsize=9
        )
        axes[1].set_title('Hot Blocks Read/Write Split', fontsize=14)
        axes[1].set_ylabel('Access Count')
        axes[1].legend(fontsize=11)
        axes[1].grid(True, axis='y', alpha=0.3)

        plt.tight_layout()
        self._save_fig(save_dir, 'hotspot_distribution.png')

    def generate_html_report(self, analysis_results, save_dir=None):
        """
        生成负载分析 HTML 综合报表
        """
        save_dir = save_dir or self.save_dir
        if not save_dir:
            return

        chart_dir = os.path.join(save_dir, 'load_charts')
        chart_files = sorted(os.listdir(chart_dir)) if os.path.exists(chart_dir) else []

        report_path = os.path.join(save_dir, 'load_report.html')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        profiles = analysis_results.get('device_profiles')
        patterns = analysis_results.get('access_patterns')
        ts_global = analysis_results.get('time_series_global')

        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>设备负载分析报表</title>
    <style>
        body {{ font-family: -apple-system, 'Segoe UI', Arial, sans-serif; max-width: 1400px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
        h1 {{ color: #1a1a2e; border-bottom: 3px solid #16213e; padding-bottom: 10px; }}
        h2 {{ color: #16213e; margin-top: 30px; }}
        .card {{ background: white; border-radius: 8px; padding: 20px; margin: 15px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .stats {{ display: flex; flex-wrap: wrap; gap: 15px; }}
        .stat-item {{ background: #e8f4f8; padding: 15px 20px; border-radius: 6px; min-width: 180px; }}
        .stat-item .label {{ color: #666; font-size: 13px; }}
        .stat-item .value {{ color: #1a1a2e; font-size: 22px; font-weight: bold; margin-top: 4px; }}
        .chart-img {{ max-width: 100%; height: auto; border-radius: 6px; margin: 10px 0; }}
        table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
        th, td {{ padding: 10px 15px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: #16213e; color: white; }}
        tr:hover {{ background: #f0f0f0; }}
        .timestamp {{ color: #888; font-size: 12px; }}
        .warning {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 10px 15px; margin: 10px 0; border-radius: 4px; }}
    </style>
</head>
<body>
    <h1>设备负载分析报表</h1>
    <p class="timestamp">生成时间: {now}</p>

    <div class="card">
        <h2>数据概况</h2>
        <div class="stats">\n"""

        if profiles is not None:
            html += f"""        <div class="stat-item"><div class="label">设备总数</div><div class="value">{len(profiles)}</div></div>
        <div class="stat-item"><div class="label">总请求数</div><div class="value">{profiles['total_requests'].sum():,}</div></div>
        <div class="stat-item"><div class="label">读操作占比</div><div class="value">{patterns.get('global_read_ratio', 0):.1%}</div></div>
        <div class="stat-item"><div class="label">顺序访问比例</div><div class="value">{patterns.get('sequential_access_ratio', 0):.1%}</div></div>\n"""

        if ts_global is not None and len(ts_global) > 0:
            html += f"""        <div class="stat-item"><div class="label">时间窗口数</div><div class="value">{len(ts_global)}</div></div>
        <div class="stat-item"><div class="label">峰值 IOPS</div><div class="value">{ts_global['iops'].max():.0f}</div></div>
        <div class="stat-item"><div class="label">平均 IOPS</div><div class="value">{ts_global['iops'].mean():.0f}</div></div>
        <div class="stat-item"><div class="label">峰值吞吐</div><div class="value">{ts_global['throughput_kb'].max():.0f} KB/s</div></div>\n"""

        html += """    </div></div>\n"""

        if ts_global is not None and len(ts_global) <= 3:
            html += """    <div class="warning">
        <strong>提示：</strong>当前数据仅覆盖 {} 个时间窗口，时间趋势图可能不够丰富。
        建议增加 <code>auto_batches</code> 或 <code>chunk_size_mb</code> 以获取更多跨时间窗口的数据。
    </div>\n""".format(len(ts_global))

        if profiles is not None:
            html += """    <div class="card">
        <h2>Top 10 高负载设备</h2>
        <table><tr><th>设备ID</th><th>总请求数</th><th>读操作</th><th>写操作</th><th>读占比</th><th>平均请求大小(KB)</th><th>峰值IOPS</th><th>峰值时段</th></tr>\n"""
            for _, row in profiles.head(10).iterrows():
                html += f"""<tr><td>D{row['device_id']}</td><td>{row['total_requests']:,}</td><td>{row['read_requests']:,}</td><td>{row['write_requests']:,}</td><td>{row['read_ratio']:.1%}</td><td>{row['avg_request_size_kb']:.1f}</td><td>{row['peak_iops']:.0f}</td><td>{row['peak_hour']}:00</td></tr>\n"""
            html += "</table></div>\n"

        if chart_files:
            html += """    <div class="card">
        <h2>负载趋势图表</h2>\n"""
            for cf in chart_files:
                if cf.endswith('.png'):
                    title = cf.replace('_', ' ').replace('.png', '').title()
                    html += f'        <h3>{title}</h3>\n        <img class="chart-img" src="./load_charts/{cf}">\n'
            html += "    </div>\n"

        html += """</body></html>"""

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info(f"HTML 报表已生成: {report_path}")
