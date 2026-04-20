#!/usr/bin/env python3
"""
成熟的数据处理管道
集成数据下载、清洗和分析功能
"""

import pandas as pd
import numpy as np
import os
import json
import logging
import glob
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from src.data_download.oss_data_processor import OSSDataProcessor
from src.data_cleaning.preprocessor import DataPreprocessor
from src.data_analysis.analyzer import DataAnalyzer

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


class DataCleaningPipeline:
    """数据清洗管道"""
    
    def __init__(self, config_file=None):
        """
        初始化数据清洗管道
        Args:
            config_file: 配置文件路径
        """
        self.config = self._load_config(config_file)
        self.output_dir = self.config.get('output_dir', 'output')
        self.data_dir = 'data'  # 数据目录
        self.raw_data_dir = os.path.join(self.data_dir, 'raw')  # 原始数据目录
        self.processed_data_dir = os.path.join(self.data_dir, 'processed')  # 处理后数据目录
        self.downloader = None
        self.preprocessor = None
        self.analyzer = None
        
        # 创建目录
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)
        logger.info(f"数据清洗管道初始化完成，输出目录: {self.output_dir}，原始数据目录: {self.raw_data_dir}，处理后数据目录: {self.processed_data_dir}")
    
    def _load_config(self, config_file):
        """
        加载配置文件
        """
        default_config = {
            'oss_url': 'http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz',
            'max_retries': 3,
            'chunk_size_mb': 50,
            'download_mode': 'partial',  # stream 或 partial
            'partial_start_gb': 0,
            'partial_end_gb': 0.5,  # 限制下载数据量为500mb
            'output_dir': 'output',
            'cleaning': {
                'missing_value_strategy': 'median',
                'outlier_strategy': 'iqr'
            },
            'analysis': {
                'time_column': None,
                'target_column': None
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
        """
        初始化各个组件
        """
        # 初始化下载器
        self.downloader = OSSDataProcessor(
            self.config['oss_url'],
            max_retries=self.config['max_retries']
        )
        
        # 初始化预处理器
        self.preprocessor = DataPreprocessor(self.config.get('cleaning', {}))
        
        # 初始化分析器
        self.analyzer = DataAnalyzer()
        
        logger.info("组件初始化完成")
    
    def check_existing_data(self):
        """
        检查是否存在已下载的数据
        """
        if self.config['download_mode'] == 'partial':
            expected_file = os.path.join(
                self.raw_data_dir,
                f"partial_{self.config['partial_start_gb']}gb_to_{self.config['partial_end_gb']}gb.tar.gz"
            )
            if os.path.exists(expected_file):
                logger.info(f"发现已下载的数据文件: {expected_file}")
                return expected_file
            else:
                logger.info("未发现已下载的数据文件")
                return None
        else:
            # 检查是否存在处理后的文件
            processed_files = glob.glob(os.path.join(self.processed_data_dir, "cleaned_data_*.csv"))
            if processed_files:
                latest_file = max(processed_files, key=os.path.getmtime)
                logger.info(f"发现已处理的数据文件: {latest_file}")
                return latest_file
            else:
                logger.info("未发现已处理的数据文件")
                return None

    def download_data(self):
        """
        下载数据
        """
        # 检查是否存在已下载的数据
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
            logger.info(f"下载部分数据到: {output_file}")
            
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
        """
        清洗数据
        """
        cleaned_chunk = self.preprocessor.run_pipeline(
            data_chunk,
            time_column=self.config['analysis'].get('time_column'),
            target_column=self.config['analysis'].get('target_column')
        )
        return cleaned_chunk

    def analyze_data(self, data):
        """
        分析数据
        """
        self.analyzer.comprehensive_analysis(
            data,
            time_column=self.config['analysis'].get('time_column')
        )

    def visualize_data(self, data, viz_count=0):
        """
        可视化数据并生成报表
        Args:
            data: 要可视化的数据
            viz_count: 当前可视化次数，用于限制可视化次数
        """
        # 只在第1、2、3次处理时进行可视化
        if viz_count in [0, 1, 2]:
            # 创建可视化目录
            viz_dir = os.path.join(self.output_dir, 'visualization')
            os.makedirs(viz_dir, exist_ok=True)
            
            # 设置中文字体
            plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
            plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
            
            # 生成时间戳
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # 1. 数据分布直方图
            for col in data.select_dtypes(include=[np.number]).columns:
                plt.figure(figsize=(10, 6))
                sns.histplot(data[col], kde=True)
                plt.title(f'{col} 数据分布')
                plt.savefig(os.path.join(viz_dir, f'{col}_distribution_{timestamp}.png'))
                plt.close()
            
            # 2. 相关性热图
            if len(data.select_dtypes(include=[np.number]).columns) > 1:
                plt.figure(figsize=(12, 10))
                correlation = data.corr()
                sns.heatmap(correlation, annot=True, cmap='coolwarm', fmt='.2f')
                plt.title('数据相关性热图')
                plt.savefig(os.path.join(viz_dir, f'correlation_heatmap_{timestamp}.png'))
                plt.close()
            
            # 3. 箱线图
            plt.figure(figsize=(12, 8))
            sns.boxplot(data=data.select_dtypes(include=[np.number]))
            plt.title('数据箱线图')
            plt.xticks(rotation=45)
            plt.savefig(os.path.join(viz_dir, f'boxplot_{timestamp}.png'))
            plt.close()
            
            # 4. 生成HTML报表
            self._generate_html_report(viz_dir, timestamp)
            
            logger.info(f"数据可视化完成，结果保存在: {viz_dir}")
    
    def _generate_html_report(self, viz_dir, timestamp):
        """
        生成HTML报表
        """
        report_file = os.path.join(viz_dir, f'report_{timestamp}.html')
        
        # 收集所有生成的图表
        image_files = glob.glob(os.path.join(viz_dir, f'*.png'))
        
        # 生成HTML内容
        html_content = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>数据可视化报表</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                .chart-container {{ margin: 20px 0; padding: 20px; border: 1px solid #ddd; }}
                img {{ max-width: 100%; height: auto; }}
            </style>
        </head>
        <body>
            <h1>数据可视化报表</h1>
            <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        """
        
        # 添加图表
        for img_file in image_files:
            img_name = os.path.basename(img_file)
            html_content += f"""
            <div class="chart-container">
                <h2>{img_name.replace('_', ' ').replace('.png', '')}</h2>
                <img src="./{img_name}" alt="{img_name}">
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        
        # 保存HTML文件
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML报表生成完成: {report_file}")

    def process_data(self, max_batches=None):
        """
        处理数据
        Args:
            max_batches: 最大处理批次数量，如果为None则处理所有数据
        """
        processed_data = []
        batch_count = 0
        
        def processing_callback(data_chunk, filename, chunk_num):
            """
            处理回调函数
            """
            nonlocal batch_count
            batch_count += 1
            
            logger.info(f"处理批次 {batch_count} - {filename} - 块 {chunk_num}")
            
            # 清洗数据
            cleaned_chunk = self.clean_data(data_chunk)
            
            # 分析数据
            self.analyze_data(cleaned_chunk)
            
            # 可视化数据
            self.visualize_data(cleaned_chunk, viz_count=batch_count-1)
            
            processed_data.append(cleaned_chunk)
            
            # 保存中间结果
            intermediate_file = os.path.join(
                self.processed_data_dir,
                f"processed_{os.path.basename(filename)}_chunk{chunk_num}.csv"
            )
            cleaned_chunk.to_csv(intermediate_file, index=False)
            logger.info(f"中间结果保存到: {intermediate_file}")
            
            # 检查处理质量
            quality_score = self._check_data_quality(cleaned_chunk)
            logger.info(f"批次 {batch_count} 处理质量得分: {quality_score}")
            
            # 处理达标后提示用户
            if quality_score >= 0.8:
                logger.info(f"批次 {batch_count} 处理质量达标，得分: {quality_score}")
                print(f"\n[提示] 批次 {batch_count} 处理质量达标，得分: {quality_score}")
                print("是否继续处理下一批次？(y/n): ")
                user_input = input().strip().lower()
                if user_input != 'y':
                    logger.info("用户选择停止处理")
                    return False
            
            # 检查是否达到最大批次数量
            if max_batches and batch_count >= max_batches:
                logger.info(f"已达到最大批次数量 {max_batches}")
                return False
            
            return True
        
        try:
            if self.config['download_mode'] == 'partial':
                # 处理已下载的本地文件
                local_file = os.path.join(
                    self.raw_data_dir,
                    f"partial_{self.config['partial_start_gb']}gb_to_{self.config['partial_end_gb']}gb.tar.gz"
                )
                if os.path.exists(local_file):
                    logger.info(f"处理本地文件: {local_file}")
                    # 这里需要实现处理本地tar.gz文件的逻辑
                    # 暂时使用stream_process，但需要修改它以支持本地文件
                    self.downloader.stream_process(
                        processing_callback,
                        chunk_size_mb=self.config['chunk_size_mb'],
                        local_file=local_file
                    )
                else:
                    logger.error(f"本地文件不存在: {local_file}")
                    return None
            else:
                # 流式处理
                self.downloader.stream_process(
                    processing_callback,
                    chunk_size_mb=self.config['chunk_size_mb']
                )
            
            # 合并所有处理后的数据
            if processed_data:
                combined_df = pd.concat(processed_data, ignore_index=True)
                output_file = os.path.join(
                    self.processed_data_dir,
                    f"cleaned_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
                combined_df.to_csv(output_file, index=False)
                logger.info(f"清洗后的数据已保存到: {output_file}")
                return output_file
            else:
                logger.warning("没有处理的数据")
                return None
        except Exception as e:
            logger.error(f"处理数据失败: {e}")
            return None
    
    def _check_data_quality(self, data):
        """
        检查数据处理质量
        返回质量得分 (0-1)
        """
        # 计算非空值比例
        non_null_ratio = data.notnull().mean().mean()
        
        # 计算数据范围合理性（这里简化处理，实际应根据具体业务逻辑调整）
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        range_score = 0
        if len(numeric_cols) > 0:
            # 检查是否有极端值
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
        
        # 综合得分
        quality_score = (non_null_ratio + range_score) / 2
        return quality_score
    
    def run(self, steps=None, max_batches=None):
        """
        运行数据清洗管道
        Args:
            steps: 要运行的步骤列表，可选值：['download', 'clean', 'analyze', 'visualize']
                  如果为None，则运行所有步骤
            max_batches: 最大处理批次数量，如果为None则处理所有数据
        """
        if steps is None:
            steps = ['download', 'clean', 'analyze', 'visualize']
        
        try:
            # 初始化组件
            self.initialize_components()
            
            # 下载数据
            if 'download' in steps:
                download_result = self.download_data()
                if not download_result:
                    logger.error("数据下载失败")
                    return False
            
            # 处理数据
            if 'clean' in steps or 'analyze' in steps or 'visualize' in steps:
                output_file = self.process_data(max_batches=max_batches)
                if output_file:
                    logger.info(f"处理完成，输出文件: {output_file}")
                    return True
                else:
                    logger.error("数据处理失败")
                    return False
            else:
                # 只下载数据，不需要处理
                logger.info("数据下载完成")
                return True
        except Exception as e:
            logger.error(f"运行失败: {e}")
            return False
        finally:
            # 清理资源
            if self.downloader:
                self.downloader.cleanup()


def main():
    """
    主函数
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='数据清洗管道')
    parser.add_argument('--config', help='配置文件路径')
    parser.add_argument('--url', help='OSS文件URL')
    parser.add_argument('--mode', choices=['stream', 'partial'], help='下载模式')
    parser.add_argument('--output', help='输出目录')
    parser.add_argument('--steps', nargs='+', choices=['download', 'clean', 'analyze', 'visualize'], 
                        help='要运行的步骤列表')
    parser.add_argument('--max-batches', type=int, help='最大处理批次数量')
    
    args = parser.parse_args()
    
    # 创建管道实例
    pipeline = DataCleaningPipeline(args.config)
    
    # 覆盖配置
    if args.url:
        pipeline.config['oss_url'] = args.url
    if args.mode:
        pipeline.config['download_mode'] = args.mode
    if args.output:
        pipeline.config['output_dir'] = args.output
    
    # 运行管道
    success = pipeline.run(steps=args.steps, max_batches=args.max_batches)
    
    if success:
        logger.info("数据清洗管道执行成功")
    else:
        logger.error("数据清洗管道执行失败")


if __name__ == "__main__":
    main()
