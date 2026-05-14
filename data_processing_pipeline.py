#!/usr/bin/env python3
"""
数据处理管道（编排器）
集成数据下载、清洗、设备负载分析、预测功能
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
from src.models.predictor import TimeSeriesPredictor

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
        self.predictor = None

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
            },
            'prediction': {
                'targets': ['iops', 'throughput_kb'],
                'models': ['linear', 'ridge', 'ar1', 'exp_smoothing',
                           'random_forest', 'gradient_boosting', 'trend', 'moving_avg'],
                'n_lags': None,
                'predict_steps': 2
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
        self.predictor = TimeSeriesPredictor(self.config.get('prediction', {}))
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

    # ================================================================
    # 独立预测步骤（可反复重跑）
    # ================================================================

    def run_prediction(self):
        """
        独立的负载预测步骤 — 可反复重跑

        从 data/processed/processed_io_traces*.csv 加载原始 I/O 数据，
        自动指定列名并构造 datetime 列，然后调用 TimeSeriesPredictor。

        Returns:
            预测目录路径，失败返回 None
        """
        io_chunks = sorted(glob.glob(
            os.path.join(self.processed_data_dir, 'processed_io_traces*.csv')
        ))

        if io_chunks:
            logger.info(f"从 {len(io_chunks)} 个 I/O 轨迹块加载数据...")
            chunks = []
            for f in io_chunks:
                # CSV 无表头，指定 header=None
                df_chunk = pd.read_csv(f, low_memory=False, header=None)
                chunks.append(df_chunk)
            df = pd.concat(chunks, ignore_index=True)
            # 指定列名：序号, R/W, device_id, size, timestamp, ...
            col_count = df.shape[1]
            names = ['row_idx', 'operation', 'device_id', 'size', 'timestamp']
            if col_count > 5:
                names += [f'col_{i}' for i in range(5, col_count)]
            df.columns = names
            logger.info(f"已指定列名: {names[:5]}")
        else:
            cleaned_files = sorted(glob.glob(
                os.path.join(self.processed_data_dir, 'cleaned_data_*.csv')
            ))
            if not cleaned_files:
                logger.error("没有找到 I/O 轨迹数据，请先运行 load_analysis")
                return None
            logger.info(f"使用合并数据: {cleaned_files[-1]}")
            df = pd.read_csv(cleaned_files[-1], low_memory=False)

        logger.info(f"预测数据: {len(df)} 行")

        # 构造 datetime 列
        if 'datetime' not in df.columns and 'timestamp' in df.columns:
            for unit in ['us', 'ms', 's']:
                try:
                    df['datetime'] = pd.to_datetime(df['timestamp'], unit=unit, errors='raise')
                    logger.info(f"  datetime 构造成功 (unit={unit})")
                    break
                except Exception:
                    pass
            if 'datetime' not in df.columns:
                logger.error("无法从 timestamp 构造 datetime 列")
                return None

        if len(df) < 10:
            logger.error(f"数据量不足: {len(df)} < 10")
            return None

        logger.info(f"数据准备完成: {len(df)} 行, 列: {list(df.columns[:6])}")

        prediction_dir = os.path.join(self.output_dir, 'prediction')
        os.makedirs(prediction_dir, exist_ok=True)

        logger.info("=" * 60)
        logger.info("开始负载预测")
        logger.info("=" * 60)

        result = self.predict_load(df, save_dir=prediction_dir)

        if result:
            logger.info("=" * 60)
            logger.info("负载预测完成")
            logger.info("=" * 60)
        return prediction_dir if result else None

    # ================================================================
    # 回归测试（可反复重跑）
    # ================================================================

    def run_regression_test(self):
        """
        运行回归测试 — 反复可重用的多模型回归性能评估

        使用 device_analysis/device_profiles.csv 数据，对多个回归模型
        (Linear, Ridge, RandomForest, GradientBoosting) 进行评估，
        生成偏差可视化图表和 HTML 报告，结果保存到 output/regression_analysis/。

        Returns:
            回归分析目录路径
        """
        profiles_path = os.path.join(self.output_dir, 'device_analysis', 'device_profiles.csv')
        if not os.path.exists(profiles_path):
            logger.error(f"设备画像不存在: {profiles_path}，请先运行 load_analysis")
            return None

        profiles_df = pd.read_csv(profiles_path)
        if len(profiles_df) < 10:
            logger.error(f"数据量不足: {len(profiles_df)} < 10")
            return None

        regression_dir = os.path.join(self.output_dir, 'regression_analysis')
        os.makedirs(regression_dir, exist_ok=True)
        logger.info("=" * 60)
        logger.info("开始回归测试")
        logger.info("=" * 60)

        try:
            from src.device_load_analysis.load_reporter import LoadReporter
            reporter = LoadReporter(save_dir=regression_dir)
            reporter.visualize_regression_test(profiles_df, save_dir=regression_dir)
            logger.info(f"回归测试完成，结果保存在: {regression_dir}")
        except Exception as e:
            logger.error(f"回归测试失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

        logger.info("=" * 60)
        logger.info("回归测试完成")
        logger.info("=" * 60)
        return regression_dir

    def predict_load(self, df_raw_io, save_dir=None):
        """
        基于原始 I/O 数据进行回归预测

        Args:
            df_raw_io: 原始 I/O 轨迹 DataFrame
            save_dir: 可视化保存目录

        Returns:
            prediction_results: dict
        """
        if self.predictor is None:
            self.predictor = TimeSeriesPredictor(self.config.get('prediction', {}))

        pred_config = self.config.get('prediction', {})
        targets = pred_config.get('targets', ['iops', 'throughput_kb'])
        models = pred_config.get('models', None)
        n_lags = pred_config.get('n_lags', None)
        predict_steps = pred_config.get('predict_steps', 2)

        if save_dir is None:
            save_dir = os.path.join(self.output_dir, 'prediction')
        os.makedirs(save_dir, exist_ok=True)

        logger.info("=" * 60)
        logger.info(f"开始负载预测: 目标={targets}, 预测步数={predict_steps}")
        logger.info("=" * 60)

        # ---- 数据预处理：确保包含必要列 ----
        df = df_raw_io.copy()
        required_cols = {'datetime', 'operation', 'device_id', 'size'}
        missing = required_cols - set(df.columns)
        if missing:
            logger.warning(f"数据缺少列 {missing}，尝试自动推断...")
            cols = list(df.columns)
            # 尝试推断 operation 列（第2列，值为 R/W）
            if 'operation' not in df.columns and len(cols) > 1:
                sample = str(df.iloc[0, 1]) if len(df) > 0 else ''
                if sample.upper() in ['R', 'W']:
                    df = df.rename(columns={df.columns[1]: 'operation'})
                    missing.discard('operation')
            # 推断 device_id（第3列，大整数）
            if 'device_id' not in df.columns and len(cols) > 2:
                try:
                    df = df.rename(columns={df.columns[2]: 'device_id'})
                    missing.discard('device_id')
                except Exception:
                    pass
            # 推断 size（第4列，整数/浮点数）
            if 'size' not in df.columns and len(cols) > 3:
                try:
                    df = df.rename(columns={df.columns[3]: 'size'})
                    missing.discard('size')
                except Exception:
                    pass
            # 从 timestamp 构造 datetime
            if 'datetime' not in df.columns and 'timestamp' in df.columns:
                logger.info("从 timestamp 列构造 datetime...")
                ts_col = df['timestamp']
                # 尝试毫秒/微秒/秒
                for unit in ['ms', 'us', 's']:
                    try:
                        df['datetime'] = pd.to_datetime(ts_col, unit=unit, errors='raise')
                        logger.info(f"  datetime 构造成功 (unit={unit})")
                        missing.discard('datetime')
                        break
                    except Exception:
                        pass
            if missing:
                logger.error(f"数据仍缺少必要列: {missing}，当前列: {list(df.columns)}")
                logger.error("请确保传入的是原始 I/O 轨迹数据（含 datetime/timestamp 列）")
                return None

        # 确保数据量足够
        if len(df) < 10:
            logger.error(f"数据量不足: {len(df)} < 10")
            return None

        logger.info(f"预测数据: {len(df)} 行, 列: {list(df.columns[:8])}")

        try:
            all_results = self.predictor.run_multi_target_prediction(
                df, targets=targets, model_names=models,
                n_lags=n_lags, predict_steps=predict_steps
            )

            # 打印报告
            self.predictor.print_report(all_results)

            # 生成可视化
            self.predictor.visualize_predictions(all_results, df, save_dir)

            logger.info(f"预测完成，结果保存至: {save_dir}")
            return all_results
        except Exception as e:
            logger.error(f"预测失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None


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
            ts_global = None
            raw_combined = None
            if raw_io_traces:
                logger.info(f"合并 {len(raw_io_traces)} 个批次的I/O轨迹数据进行负载分析...")
                raw_combined = pd.concat(raw_io_traces, ignore_index=True)
                load_analysis_dir = os.path.join(self.output_dir, 'device_analysis')
                try:
                    self.load_analyzer.run_full_analysis(raw_combined, save_dir=load_analysis_dir)
                    # 获取全局时间序列用于预测
                    ts_global = self.load_analyzer.analysis_results.get('time_series_global')
                except KeyError as e:
                    logger.error(f"负载分析失败，列名不匹配: {e}")
                    logger.info(f"原始数据列名: {list(raw_combined.columns)}, 类型: {[type(c).__name__ for c in raw_combined.columns]}")
                except Exception as e:
                    logger.error(f"负载分析失败: {e}")

            # ---- 负载预测 ----
            if raw_combined is not None and len(raw_combined) >= 100:
                try:
                    prediction_dir = os.path.join(self.output_dir, 'prediction')
                    self.predict_load(raw_combined, save_dir=prediction_dir)
                except Exception as e:
                    logger.error(f"负载预测失败: {e}")

            return output_file if combined_df is not None else None

        except Exception as e:
            logger.error(f"处理数据失败: {e}")
            return None

    def run(self, steps=None, max_batches=None):
        """
        运行数据清洗管道

        Args:
            steps: 要运行的步骤列表 ['download', 'clean', 'analyze', 'visualize',
                   'load_analysis', 'predict']
            max_batches: 最大处理批次数量，默认自动执行 auto_batches(5) 个批次
        """
        if steps is None:
            steps = ['download', 'clean', 'analyze', 'visualize', 'load_analysis', 'predict']

        try:
            self.initialize_components()

            if 'download' in steps:
                download_result = self.download_data()
                if not download_result:
                    logger.error("数据下载失败")
                    return False

            if 'clean' in steps or 'analyze' in steps or 'visualize' in steps \
                    or 'load_analysis' in steps:
                output_file = self.process_data(max_batches=max_batches)
                if output_file:
                    logger.info(f"处理完成，输出文件: {output_file}")
                elif 'load_analysis' not in steps:
                    logger.error("数据处理失败")
                    return False
                else:
                    logger.info("数据处理阶段完成（仅分析/预测模式）")

            # 预测步骤（独立可重跑）
            if 'predict' in steps:
                pred_dir = self.run_prediction()
                if pred_dir:
                    logger.info(f"预测完成，结果保存在: {pred_dir}")
                else:
                    logger.warning("预测步骤未产生结果")

            # 回归测试步骤（可独立重跑）
            if 'regression_test' in steps:
                reg_dir = self.run_regression_test()
                if reg_dir:
                    logger.info(f"回归测试完成，结果保存在: {reg_dir}")
                else:
                    logger.warning("回归测试未产生结果")

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
                        choices=['download', 'clean', 'analyze', 'visualize',
                                 'load_analysis', 'predict', 'regression_test'],
                        help='要运行的步骤列表')
    parser.add_argument('--max-batches', type=int, help='最大处理批次数量')
    parser.add_argument('--auto-batches', type=int,
                        help=f'自动执行的批次数（默认{DEFAULT_AUTO_BATCHES}）')
    parser.add_argument('--predict-steps', type=int,
                        help='预测步数（默认2，即预测后2个时间窗口）')
    parser.add_argument('--predict-targets', nargs='+',
                        help='预测目标指标列表')

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
    if args.predict_steps:
        pipeline.config['prediction']['predict_steps'] = args.predict_steps
    if args.predict_targets:
        pipeline.config['prediction']['targets'] = args.predict_targets

    success = pipeline.run(steps=args.steps, max_batches=args.max_batches)

    if success:
        logger.info("数据清洗管道执行成功")
    else:
        logger.error("数据清洗管道执行失败")


if __name__ == "__main__":
    main()
