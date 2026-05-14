#!/usr/bin/env python3
"""
数据处理管道（编排器）
集成数据下载、清洗、设备负载分析功能
"""

import pandas as pd
import numpy as np
import os
import json
import logging
import glob
from datetime import datetime
from src.data_download.oss_data_processor import OSSDataProcessor
from src.data_cleaning.preprocessor import DataPreprocessor
from src.data_analysis.analyzer import DataAnalyzer
from src.device_load_analysis import DeviceLoadAnalyzer

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 默认自动批次数
DEFAULT_AUTO_BATCHES = 5


class DataCleaningPipeline:
    """数据清洗管道"""

    def __init__(self, config_file=None):
        self.config = self._load_config(config_file)
        self.output_dir = self.config.get('output_dir', 'output')
        self.data_dir = 'data'
        self.raw_data_dir = os.path.join(self.data_dir, 'raw')
        self.processed_data_dir = os.path.join(self.data_dir, 'processed')
        self.downloader = None
        self.preprocessor = None
        self.analyzer = None
        self.load_analyzer = None

        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)

    def _load_config(self, config_file):
        """加载配置文件"""
        default_config = {
            'oss_url': 'http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz',
            'max_retries': 3,
            'chunk_size_mb': 50,
            'download_mode': 'partial',
            'partial_start_gb': 0,
            'partial_end_gb': 0.5,
            'output_dir': 'output',
            'auto_batches': DEFAULT_AUTO_BATCHES,
            'cleaning': {
                'missing_value_strategy': 'median',
                'outlier_strategy': 'iqr'
            },
            'analysis': {
                'time_column': None,
                'target_column': None
            },
            'load_analysis': {
                'time_window': 60,
                'top_n_devices': None,
                'hotspot_threshold': 100
            }
        }

        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)
                default_config.update(user_config)
                logger.info(f"加载配置文件: {config_file}")
            except Exception as e:
                logger.error(f"加载配置文件失败: {e}")

        return default_config

    def initialize_components(self):
        """初始化各组件"""
        self.downloader = OSSDataProcessor(
            self.config['oss_url'],
            max_retries=self.config['max_retries']
        )
        self.preprocessor = DataPreprocessor(self.config.get('cleaning', {}))
        self.analyzer = DataAnalyzer()
        self.load_analyzer = DeviceLoadAnalyzer(self.config.get('load_analysis', {}))
        logger.info("组件初始化完成")

    def check_existing_data(self):
        """检查是否存在已下载的数据"""
        if self.config['download_mode'] == 'partial':
            expected_file = os.path.join(
                self.raw_data_dir,
                f"partial_{self.config['partial_start_gb']}gb_to_{self.config['partial_end_gb']}gb.tar.gz"
            )
            if os.path.exists(expected_file):
                logger.info(f"发现已下载的数据文件: {expected_file}")
                return expected_file
        else:
            processed_files = glob.glob(os.path.join(self.processed_data_dir, "cleaned_data_*.csv"))
            if processed_files:
                latest_file = max(processed_files, key=os.path.getmtime)
                logger.info(f"发现已处理的数据文件: {latest_file}")
                return latest_file
        return None

    def download_data(self):
        """下载数据"""
        existing_data = self.check_existing_data()
        if existing_data:
            logger.info("使用已下载的数据，跳过下载步骤")
            return existing_data

        download_mode = self.config['download_mode']

        if download_mode == 'stream':
            logger.info("使用流式处理模式")
            return True
        elif download_mode == 'partial':
            output_file = os.path.join(
                self.raw_data_dir,
                f"partial_{self.config['partial_start_gb']}gb_to_{self.config['partial_end_gb']}gb.tar.gz"
            )
            try:
                self.downloader.download_partial(
                    self.config['partial_start_gb'],
                    self.config['partial_end_gb'],
                    output_file
                )
                logger.info("部分数据下载完成")
                return output_file
            except Exception as e:
                logger.error(f"下载失败: {e}")
                return False
        else:
            logger.error(f"不支持的下载模式: {download_mode}")
            return False

    def clean_data(self, data_chunk):
        """清洗数据"""
        return self.preprocessor.run_pipeline(
            data_chunk,
            time_column=self.config['analysis'].get('time_column'),
            target_column=self.config['analysis'].get('target_column')
        )

    def analyze_data(self, data):
        """分析数据"""
        self.analyzer.comprehensive_analysis(
            data,
            time_column=self.config['analysis'].get('time_column')
        )

    def analyze_device_load(self, raw_df, save_dir=None):
        """设备负载分析"""
        if save_dir is None:
            save_dir = os.path.join(self.output_dir, 'device_analysis')
        return self.load_analyzer.run_full_analysis(raw_df, save_dir=save_dir)

    def _is_io_trace_data(self, df):
        """判断数据是否为I/O轨迹数据"""
        if len(df.columns) < 5:
            return False
        # 检查前5列中是否有R/W列
        check_cols = df.iloc[:, :5] if len(df.columns) >= 5 else df
        for i in range(len(check_cols.columns)):
            unique_vals = check_cols.iloc[:, i].dropna().astype(str).unique()
            if len(unique_vals) <= 2 and any(v.upper() in ['R', 'W'] for v in unique_vals):
                return True
        return False

    def _check_data_quality(self, data):
        """检查数据处理质量 (0-1)"""
        non_null_ratio = data.notnull().mean().mean()
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        range_score = 0
        if len(numeric_cols) > 0:
            for col in numeric_cols:
                q1 = data[col].quantile(0.25)
                q3 = data[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                outlier_ratio = ((data[col] < lower_bound) | (data[col] > upper_bound)).mean()
                range_score += (1 - outlier_ratio)
            range_score /= len(numeric_cols)
        else:
            range_score = 1
        return (non_null_ratio + range_score) / 2

    def process_data(self, max_batches=None):
        """
        处理数据

        流程: 自动执行 auto_batches（默认5）个批次，单批次仅日志+清洗+保存中间结果，
        不做可视化。所有批次完成后，合并原始I/O数据执行设备负载分析并输出可视化。
        """
        auto_batches = max_batches or self.config.get('auto_batches', DEFAULT_AUTO_BATCHES)
        processed_data = []
        raw_io_traces = []
        batch_count = 0

        def processing_callback(data_chunk, filename, chunk_num):
            """处理回调：清洗 + 日志统计，不做可视化"""
            nonlocal batch_count
            batch_count += 1

            logger.info(f"[批次 {batch_count}/{auto_batches}] {filename} - 块 {chunk_num}, "
                        f"行数: {len(data_chunk)}")

            # 收集原始I/O轨迹数据
            if 'io_traces' in filename.lower() or self._is_io_trace_data(data_chunk):
                raw_io_traces.append(data_chunk.copy())

            # 清洗数据
            cleaned_chunk = self.clean_data(data_chunk)

            # 简要分析（仅打印统计摘要）
            self.analyze_data(cleaned_chunk)

            processed_data.append(cleaned_chunk)

            # 保存中间结果
            intermediate_file = os.path.join(
                self.processed_data_dir,
                f"processed_{os.path.basename(filename)}_chunk{chunk_num}.csv"
            )
            cleaned_chunk.to_csv(intermediate_file, index=False)

            quality_score = self._check_data_quality(cleaned_chunk)
            logger.info(f"  质量得分: {quality_score:.2f}, "
                        f"清洗后: {len(cleaned_chunk)} 行, "
                        f"设备数: {cleaned_chunk.iloc[:, 0].nunique() if len(cleaned_chunk) > 0 else 0}")

            # 达到目标批次数自动停止
            if batch_count >= auto_batches:
                logger.info(f"已自动完成 {auto_batches} 个批次，准备合并分析")
                return False

            return True

        # ---- 执行处理 ----
        try:
            if self.config['download_mode'] == 'partial':
                local_file = os.path.join(
                    self.raw_data_dir,
                    f"partial_{self.config['partial_start_gb']}gb_to_{self.config['partial_end_gb']}gb.tar.gz"
                )
                if os.path.exists(local_file):
                    logger.info(f"处理本地文件: {local_file}")
                    self.downloader.stream_process(
                        processing_callback,
                        chunk_size_mb=self.config['chunk_size_mb'],
                        local_file=local_file
                    )
                else:
                    logger.error(f"本地文件不存在: {local_file}")
                    return None
            else:
                self.downloader.stream_process(
                    processing_callback,
                    chunk_size_mb=self.config['chunk_size_mb']
                )

            # ---- 合并处理后的数据 ----
            combined_df = None
            if processed_data:
                combined_df = pd.concat(processed_data, ignore_index=True)
                output_file = os.path.join(
                    self.processed_data_dir,
                    f"cleaned_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
                combined_df.to_csv(output_file, index=False)
                logger.info(f"清洗后合并数据已保存: {output_file}")

            # ---- 设备负载分析 + 可视化 ----
            if raw_io_traces:
                logger.info(f"合并 {len(raw_io_traces)} 个批次的I/O轨迹数据进行负载分析...")
                raw_combined = pd.concat(raw_io_traces, ignore_index=True)
                load_analysis_dir = os.path.join(self.output_dir, 'device_analysis')
                try:
                    self.load_analyzer.run_full_analysis(raw_combined, save_dir=load_analysis_dir)
                except KeyError as e:
                    logger.error(f"负载分析失败，列名不匹配: {e}")
                    logger.info(f"原始数据列名: {list(raw_combined.columns)}, 类型: {[type(c).__name__ for c in raw_combined.columns]}")
                except Exception as e:
                    logger.error(f"负载分析失败: {e}")

            return output_file if combined_df is not None else None

        except Exception as e:
            logger.error(f"处理数据失败: {e}")
            return None

    def run(self, steps=None, max_batches=None):
        """
        运行数据清洗管道

        Args:
            steps: 要运行的步骤列表 ['download', 'clean', 'analyze', 'visualize', 'load_analysis']
            max_batches: 最大处理批次数量，默认自动执行 auto_batches(5) 个批次
        """
        if steps is None:
            steps = ['download', 'clean', 'analyze', 'visualize']

        try:
            self.initialize_components()

            if 'download' in steps:
                download_result = self.download_data()
                if not download_result:
                    logger.error("数据下载失败")
                    return False

            if 'clean' in steps or 'analyze' in steps or 'visualize' in steps or 'load_analysis' in steps:
                output_file = self.process_data(max_batches=max_batches)
                if output_file:
                    logger.info(f"处理完成，输出文件: {output_file}")
                elif 'load_analysis' not in steps:
                    logger.error("数据处理失败")
                    return False
                else:
                    logger.info("数据处理阶段完成（仅负载分析模式）")
                return True
            else:
                logger.info("数据下载完成")
                return True
        except Exception as e:
            logger.error(f"运行失败: {e}")
            return False
        finally:
            if self.downloader:
                self.downloader.cleanup()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='数据清洗管道')
    parser.add_argument('--config', help='配置文件路径')
    parser.add_argument('--url', help='OSS文件URL')
    parser.add_argument('--mode', choices=['stream', 'partial'], help='下载模式')
    parser.add_argument('--output', help='输出目录')
    parser.add_argument('--steps', nargs='+',
                        choices=['download', 'clean', 'analyze', 'visualize', 'load_analysis'],
                        help='要运行的步骤列表')
    parser.add_argument('--max-batches', type=int, help='最大处理批次数量')
    parser.add_argument('--auto-batches', type=int,
                        help=f'自动执行的批次数（默认{DEFAULT_AUTO_BATCHES}）')

    args = parser.parse_args()

    pipeline = DataCleaningPipeline(args.config)

    if args.url:
        pipeline.config['oss_url'] = args.url
    if args.mode:
        pipeline.config['download_mode'] = args.mode
    if args.output:
        pipeline.config['output_dir'] = args.output
    if args.auto_batches:
        pipeline.config['auto_batches'] = args.auto_batches

    success = pipeline.run(steps=args.steps, max_batches=args.max_batches)

    if success:
        logger.info("数据清洗管道执行成功")
    else:
        logger.error("数据清洗管道执行失败")


if __name__ == "__main__":
    main()
