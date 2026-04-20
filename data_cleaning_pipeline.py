#!/usr/bin/env python3
"""
成熟的数据清洗管道
集成数据下载、清洗和分析功能
"""

import pandas as pd
import numpy as np
import os
import json
import logging
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
        self.downloader = None
        self.preprocessor = None
        self.analyzer = None
        
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"数据清洗管道初始化完成，输出目录: {self.output_dir}")
    
    def _load_config(self, config_file):
        """
        加载配置文件
        """
        default_config = {
            'oss_url': 'http://block-traces.oss-cn-beijing.aliyuncs.com/alibaba_block_traces_2020.tar.gz',
            'max_retries': 3,
            'chunk_size_mb': 50,
            'download_mode': 'stream',  # stream 或 partial
            'partial_start_gb': 0,
            'partial_end_gb': 1,
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
    
    def download_data(self):
        """
        下载数据
        """
        download_mode = self.config['download_mode']
        
        if download_mode == 'stream':
            logger.info("使用流式处理模式")
            return True
        elif download_mode == 'partial':
            output_file = os.path.join(
                self.output_dir,
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
                return True
            except Exception as e:
                logger.error(f"下载失败: {e}")
                return False
        else:
            logger.error(f"不支持的下载模式: {download_mode}")
            return False
    
    def process_data(self):
        """
        处理数据
        """
        processed_data = []
        
        def processing_callback(data_chunk, filename, chunk_num):
            """
            处理回调函数
            """
            logger.info(f"处理 {filename} - 块 {chunk_num}")
            
            # 清洗数据
            cleaned_chunk = self.preprocessor.run_pipeline(
                data_chunk,
                time_column=self.config['analysis'].get('time_column'),
                target_column=self.config['analysis'].get('target_column')
            )
            
            # 分析数据
            self.analyzer.comprehensive_analysis(
                cleaned_chunk,
                time_column=self.config['analysis'].get('time_column')
            )
            
            processed_data.append(cleaned_chunk)
            
            # 保存中间结果
            intermediate_file = os.path.join(
                self.output_dir,
                f"processed_{os.path.basename(filename)}_chunk{chunk_num}.csv"
            )
            cleaned_chunk.to_csv(intermediate_file, index=False)
            logger.info(f"中间结果保存到: {intermediate_file}")
        
        try:
            self.downloader.stream_process(
                processing_callback,
                chunk_size_mb=self.config['chunk_size_mb']
            )
            
            # 合并所有处理后的数据
            if processed_data:
                combined_df = pd.concat(processed_data, ignore_index=True)
                output_file = os.path.join(
                    self.output_dir,
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
    
    def run(self):
        """
        运行整个清洗管道
        """
        logger.info("开始运行数据清洗管道")
        
        try:
            # 初始化组件
            self.initialize_components()
            
            # 下载数据
            if not self.download_data():
                logger.error("数据下载失败，管道终止")
                return False
            
            # 处理数据
            output_file = self.process_data()
            if output_file:
                logger.info(f"数据清洗管道运行成功，输出文件: {output_file}")
                return True
            else:
                logger.error("数据处理失败")
                return False
        except Exception as e:
            logger.error(f"管道运行失败: {e}")
            return False
        finally:
            # 清理资源
            if self.downloader:
                self.downloader.cleanup()
            logger.info("数据清洗管道运行结束")


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
    success = pipeline.run()
    
    if success:
        logger.info("数据清洗管道执行成功")
    else:
        logger.error("数据清洗管道执行失败")


if __name__ == "__main__":
    main()
