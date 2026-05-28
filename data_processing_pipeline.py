#!/usr/bin/env python3
"""
数据处理管道（编排器）— 流式两阶段处理

  --stage 1: HTTP → gzip → tar → 增量聚合 → 保存 CSV/JSON（断点续传）
  --stage 2: 加载聚合数据 → 生成图表 + 报告 + 负载预测（可反复重跑）

输出位置:
  聚合数据: data/device_analysis/ (CSV/JSON)
  可视化报告: output/device_analysis/ (HTML/PNG)
  预测结果: output/prediction/
"""

import pandas as pd
import os
import json
import logging
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

def format_bytes(size_bytes):
    """格式化字节为可读字符串"""
    if size_bytes is None:
        return '?'
    if size_bytes >= 1024 ** 3:
        return f'{size_bytes / (1024 ** 3):.2f} GB'
    elif size_bytes >= 1024 ** 2:
        return f'{size_bytes / (1024 ** 2):.2f} MB'
    elif size_bytes >= 1024:
        return f'{size_bytes / 1024:.2f} KB'
    return f'{size_bytes} B'


class DataCleaningPipeline:
    """数据处理管道 — 流式两阶段：聚合 → 可视化+预测"""

    def __init__(self, config_file=None):
        self.config = self._load_config(config_file)
        self.output_dir = self.config.get('output_dir', 'output')
        self.streaming_processor = None

        os.makedirs(self.output_dir, exist_ok=True)

    def _load_config(self, config_file):
        """加载配置文件，默认值会被 config.json 覆盖"""
        default_config = {
            'oss_url': 'http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz',
            'max_retries': 3,
            'output_dir': 'output',
            'streaming': {
                'work_dir': 'data/stream_checkpoints',
                'checkpoint_interval_rows': 5000000,  # 每500万行保存 checkpoint
                'max_retries': 3,
                'max_gb': 20,
                'sample_ratio': 1.0,
            },
            'load_analysis': {
                'time_window': 60,
                'hotspot_threshold': 100,
            },
            'prediction': {
                'targets': ['iops', 'throughput_kb'],
                'predict_steps': 2,
            },
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

    # ================================================================
    # ================================================================
    # 流式处理 — 两阶段 API
    #   阶段1 (aggregate): 下载 + 增量聚合 → 保存 CSV/JSON → data/device_analysis/
    #   阶段2 (visualize): 加载聚合数据(data/) → 图表 + 报告(output/device_analysis/) + 预测(output/prediction/)
    #   每次阶段2 可基于阶段1的结果反复重跑
    # ================================================================

    def _get_streaming_config(self):
        """获取流式处理合并后的配置"""
        streaming_config = self.config.get('streaming', {})
        load_config = self.config.get('load_analysis', {})
        # max_gb: 累计目标总量（如 20GB），跨运行累积
        max_gb = streaming_config.get('max_gb', 0)
        if max_gb <= 0:
            max_gb = self.config.get('max_gb', 0)
        if max_gb <= 0:
            max_gb = self.config.get('partial_end_gb', 0)
        return {
            'max_retries': streaming_config.get('max_retries', self.config.get('max_retries', 3)),
            'chunk_size_mb': streaming_config.get('download_chunk_mb', 10),
            'checkpoint_interval_rows': streaming_config.get('checkpoint_interval_rows', 5000000),
            'load_analysis': load_config,
            # 优化参数
            'max_rows': streaming_config.get('max_rows', 0),
            'max_gb': max_gb,
            'sample_ratio': streaming_config.get('sample_ratio', 1.0),
            'top_device_ts': streaming_config.get('top_device_ts', 50),
            'skip_second_dist': streaming_config.get('skip_second_dist', False),
        }

    def run_aggregate(self):
        """
        阶段1: 流式下载 + 增量聚合 → 保存聚合数据（CSV/JSON）

        完成后提示：
          "聚合完成！共 X 行 Y 设备。请运行 --stage 2 进行可视化。"

        Returns:
            'complete'  完全完成（数据全部处理完毕）
            'partial'   部分完成（达到 max_rows/max_gb 限制，可继续）
            False       失败
        """
        from src.data_download.streaming_processor import StreamingTarProcessor

        streaming_config = self.config.get('streaming', {})
        work_dir = streaming_config.get('work_dir', 'data/stream_checkpoints')
        url = self.config['oss_url']
        full_config = self._get_streaming_config()
        max_rows = full_config.get('max_rows', 0)
        max_gb = full_config.get('max_gb', 0)

        device_analysis_dir = os.path.join('data', 'device_analysis')
        processor = StreamingTarProcessor(url, work_dir, full_config, output_dir=device_analysis_dir)
        self.streaming_processor = processor

        try:
            logger.info("=" * 60)
            logger.info("【阶段1】流式聚合分析 — 边下载边处理（不落盘）")
            logger.info("=" * 60)
            logger.info(f"目标 URL: {url}")
            logger.info(f"工作目录: {work_dir}")
            logger.info(f"输出目录: {self.output_dir}")
            if max_gb > 0:
                logger.info(f"累计目标: {max_gb} GB（checkpoint 跨运行累积）")
            if max_rows > 0:
                logger.info(f"目标行数: {max_rows:,}")
            sample_ratio = full_config.get('sample_ratio', 1.0)
            if sample_ratio < 1.0:
                logger.info(f"采样率: {sample_ratio:.0%}")

            def _progress_callback(bytes_dl, total_size, rows_processed, phase='stream'):
                if total_size > 1:
                    progress_pct = (bytes_dl / total_size * 100)
                    logger.info(f"  [{progress_pct:.1f}%] {format_bytes(bytes_dl)}/"
                                f"{format_bytes(total_size)} | 已聚合: {rows_processed:,} 行")
                else:
                    logger.info(f"  [下载] {format_bytes(bytes_dl)} | 已聚合: {rows_processed:,} 行")

            results = processor.process(progress_callback=_progress_callback)
            total_rows = results.get('total_rows_processed', 0)
            total_devices = results.get('total_devices', 0)
            stream_ended = results.get('_stream_ended_naturally', False)

            if total_rows == 0:
                logger.error("流式处理未产生任何数据")
                return False

            # 最终保存聚合数据 + checkpoint 清理。
            # process() 内部已调用 _incremental_save 做最终保存（关掉 gap），
            # save_aggregation 再做一次完整覆盖 + 打印摘要 + 根据 stream_ended 决定是否清理 checkpoint。
            processor.save_aggregation(device_analysis_dir, keep_checkpoint=not stream_ended)

            # 判断是否部分完成（达到限制而非自然结束）
            reached_limit = False
            if max_rows > 0 and total_rows >= max_rows:
                reached_limit = True
            if max_gb > 0 and processor.decompressed_bytes >= max_gb * (1024 ** 3):
                reached_limit = True

            if reached_limit and not stream_ended:
                logger.info("=" * 60)
                logger.info("【阶段1 中断】达到处理上限，checkpoint 已保留")
                logger.info(f"  聚合行数: {total_rows:,}")
                logger.info(f"  设备数量: {total_devices}")
                logger.info(f"  数据位置: {device_analysis_dir}")
                logger.info("")
                logger.info("  >>> 继续处理: python data_processing_pipeline.py --resume")
                logger.info("  >>> 或查看当前数据: python data_processing_pipeline.py --stage 2")
                logger.info("=" * 60)
                return 'partial'

            logger.info("=" * 60)
            logger.info("【阶段1 完成】数据已全部处理")
            logger.info(f"  聚合行数: {total_rows:,}")
            logger.info(f"  设备数量: {total_devices}")
            logger.info(f"  数据位置: {device_analysis_dir}")
            logger.info("")
            logger.info("  >>> 下一步: python data_processing_pipeline.py --stage 2")
            logger.info("  >>> （生成图表 + 负载预测，可反复重跑）")
            logger.info("=" * 60)

            return 'complete'

        except KeyboardInterrupt:
            logger.info("\n用户中断，checkpoint 已保存。恢复: --resume")
            return False
        except Exception as e:
            logger.error(f"聚合失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def run_visualize_and_predict(self):
        """
        阶段2: 加载已聚合数据 → 生成图表 + 报告 + 负载预测

        基于阶段1的结果，可反复重跑，无需重新下载。
        """
        from src.data_download.streaming_processor import StreamingTarProcessor

        streaming_config = self.config.get('streaming', {})
        work_dir = streaming_config.get('work_dir', 'data/stream_checkpoints')
        url = self.config['oss_url']

        # 检查聚合数据是否存在（阶段1输出在 data/device_analysis/）
        data_dir = os.path.join('data', 'device_analysis')
        ts_global_path = os.path.join(data_dir, 'time_series_global.csv')

        if not os.path.exists(ts_global_path):
            logger.error(f"聚合数据不存在: {ts_global_path}")
            logger.error("请先运行阶段1: python data_processing_pipeline.py --stage 1")
            return False

        # 可视化输出到 output/device_analysis/
        viz_dir = os.path.join(self.output_dir, 'device_analysis')

        processor = StreamingTarProcessor(url, work_dir, self._get_streaming_config())

        try:
            # ---- 可视化 + 报告 ----
            logger.info("=" * 60)
            logger.info("【阶段2】可视化 + 报告生成")
            logger.info("=" * 60)

            results = processor.generate_visualization(data_dir, viz_dir)
            if results is None:
                return False

            ts_global = results.get('time_series_global')

            # ---- 负载预测 ----
            logger.info("=" * 60)
            logger.info("【阶段2】负载预测")
            logger.info("=" * 60)

            if ts_global is not None and len(ts_global) >= 10:
                try:
                    prediction_dir = os.path.join(self.output_dir, 'prediction')
                    os.makedirs(prediction_dir, exist_ok=True)
                    self.predictor = TimeSeriesPredictor(self.config.get('prediction', {}))
                    pred_config = self.config.get('prediction', {})
                    targets = pred_config.get('targets', ['iops', 'throughput_kb'])
                    models = pred_config.get('models', None)
                    n_lags = pred_config.get('n_lags', None)
                    predict_steps = pred_config.get('predict_steps', 2)

                    all_results = self.predictor.run_multi_target_prediction(
                        ts_global, targets=targets, model_names=models,
                        n_lags=n_lags, predict_steps=predict_steps
                    )
                    self.predictor.print_report(all_results)
                    self.predictor.visualize_predictions(all_results, ts_global, prediction_dir)
                    logger.info(f"预测完成: {prediction_dir}")
                except Exception as e:
                    logger.warning(f"预测失败（数据量可能不足）: {e}")
            else:
                logger.info(f"跳过预测：时间序列不足 ({len(ts_global) if ts_global is not None else 0} < 10)")

            logger.info("=" * 60)
            logger.info("【阶段2 完成】")
            logger.info(f"  图表报告: {viz_dir}")
            logger.info(f"  预测结果: {prediction_dir if ts_global is not None and len(ts_global) >= 10 else '跳过'}")
            logger.info("=" * 60)

            return True

        except Exception as e:
            logger.error(f"可视化/预测失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def run_streaming(self):
        """
        两阶段完整执行（兼容旧接口）。

        流程：
        1. 流式下载 + 增量聚合 — 边下载边提取分析，只保留聚合结果
        2. 生成设备负载分析图表和报告
        3. 运行负载预测（基于聚合后的时间序列）

        注意：如果阶段1因达到限制（max_rows/max_gb）而部分完成，
        不会自动执行阶段2，避免每次 --resume 都重新生成报告。
        """
        # 先执行阶段1
        stage1_result = self.run_aggregate()
        if stage1_result == False:
            logger.warning("阶段1 聚合失败或中断")
            return False

        if stage1_result == 'partial':
            # 部分完成（达到限制），不自动运行阶段2
            # 用户可手动运行 --stage 2 查看当前数据
            logger.info("阶段1 部分完成（达到上限），跳过阶段2。")
            logger.info("请用 --resume 继续处理，或用 --stage 2 基于当前数据生成报告。")
            return False

        # 再执行阶段2（仅在完全完成时）
        if not self.run_visualize_and_predict():
            logger.warning("阶段2 可视化/预测失败")
            return False

        return True



    # ================================================================
    # 回归测试（可反复重跑）
    # ================================================================

    def run_regression_test(self):
        """
        运行回归测试 — 反复可重用的多模型回归性能评估

        使用 data/device_analysis/device_profiles.csv 数据，对多个回归模型
        (Linear, Ridge, RandomForest, GradientBoosting) 进行评估，
        生成偏差可视化图表和 HTML 报告，结果保存到 output/regression_analysis/。

        Returns:
            回归分析目录路径
        """
        profiles_path = os.path.join('data', 'device_analysis', 'device_profiles.csv')
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



    def run(self, steps=None, max_batches=None, stage=None):
        """
        运行数据处理管道（仅流式模式）。

        Args:
            stage: 1=聚合, 2=可视化+预测, 默认=1+2

        Returns:
            True/False 表示管道执行成功与否
        """
        if stage == 1:
            result = self.run_aggregate()
            return result in ('complete', 'partial')
        elif stage == 2:
            return self.run_visualize_and_predict()
        else:
            return self.run_streaming()

    def resume(self):
        """从 checkpoint 恢复阶段1聚合（断点续传）"""
        from src.data_download.checkpoint_manager import CheckpointManager

        streaming_config = self.config.get('streaming', {})
        work_dir = streaming_config.get('work_dir', 'data/stream_checkpoints')
        checkpoint = CheckpointManager(work_dir)

        if not checkpoint.exists():
            logger.info("没有发现 checkpoint，将重新开始聚合")
            return self.run_aggregate()

        logger.info("从 checkpoint 恢复聚合...")
        return self.run_aggregate()

    def clear_checkpoint(self):
        """清除流式处理的 checkpoint（从头开始）"""
        from src.data_download.checkpoint_manager import CheckpointManager

        streaming_config = self.config.get('streaming', {})
        work_dir = streaming_config.get('work_dir', 'data/stream_checkpoints')
        checkpoint = CheckpointManager(work_dir)
        checkpoint.clear()

        # 同时清理临时下载文件
        temp_file = os.path.join(work_dir, 'streaming_temp.tar.gz')
        if os.path.exists(temp_file):
            os.unlink(temp_file)
            logger.info(f"已清理临时文件: {temp_file}")

        logger.info("Checkpoint 已清除，下次运行将从头开始")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='大数据处理管道 — 两阶段流式处理 20GB+ 文件',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 阶段1: 下载+聚合（持续处理直到20GB上限，支持断点续传）
  python data_processing_pipeline.py --stage 1

  # 阶段2: 可视化+预测（基于阶段1结果，可反复重跑）
  python data_processing_pipeline.py --stage 2

  # 一键跑完阶段1+2
  python data_processing_pipeline.py

  # 从 checkpoint 恢复阶段1
  python data_processing_pipeline.py --resume

  # 清除 checkpoint 重新开始
  python data_processing_pipeline.py --clear-checkpoint

  # 自定义处理上限
  python data_processing_pipeline.py --stage 1 --max-gb 10

  # 50% 采样加速测试
  python data_processing_pipeline.py --stage 1 --sample-ratio 0.5
        """
    )
    parser.add_argument('--config', help='配置文件路径（默认 config.json）')
    parser.add_argument('--url', help='OSS 文件 URL')
    parser.add_argument('--output', help='输出目录（默认 output）')
    parser.add_argument('--stage', type=int, choices=[1, 2],
                        help='阶段: 1=聚合, 2=可视化+预测（默认全部）')
    parser.add_argument('--max-rows', type=int,
                        help='最大处理行数')
    parser.add_argument('--max-gb', type=float,
                        help='累计处理上限 GB（默认 20）')
    parser.add_argument('--sample-ratio', type=float,
                        help='采样率 0~1')
    parser.add_argument('--predict-steps', type=int,
                        help='预测步数（默认 2）')
    parser.add_argument('--predict-targets', nargs='+',
                        help='预测目标指标列表')
    parser.add_argument('--resume', action='store_true',
                        help='从 checkpoint 恢复阶段1聚合')
    parser.add_argument('--clear-checkpoint', action='store_true',
                        help='清除 checkpoint，从头开始')
    parser.add_argument('--work-dir', help='checkpoint 存放位置')
    parser.add_argument('--checkpoint-interval', type=int,
                        help='每多少行保存 checkpoint（默认 500万）')

    args = parser.parse_args()

    pipeline = DataCleaningPipeline(args.config)

    if args.url:
        pipeline.config['oss_url'] = args.url
    if args.output:
        pipeline.config['output_dir'] = args.output
    if args.predict_steps:
        pipeline.config.setdefault('prediction', {})['predict_steps'] = args.predict_steps
    if args.predict_targets:
        pipeline.config.setdefault('prediction', {})['targets'] = args.predict_targets

    # 流式优化参数
    pipeline.config.setdefault('streaming', {})
    if args.work_dir:
        pipeline.config['streaming']['work_dir'] = args.work_dir
    if args.checkpoint_interval:
        pipeline.config['streaming']['checkpoint_interval_rows'] = args.checkpoint_interval
    if args.max_rows:
        pipeline.config['streaming']['max_rows'] = args.max_rows
    if args.max_gb:
        pipeline.config['streaming']['max_gb'] = args.max_gb
    if args.sample_ratio is not None:
        pipeline.config['streaming']['sample_ratio'] = args.sample_ratio

    # ---- 特殊命令 ----
    if args.clear_checkpoint:
        pipeline.clear_checkpoint()
        logger.info("Checkpoint 已清除")
        return

    if args.resume:
        success = pipeline.resume()
    else:
        success = pipeline.run(stage=args.stage)

    if success:
        logger.info("管道执行成功")
    else:
        logger.error("管道执行失败")


if __name__ == "__main__":
    main()
