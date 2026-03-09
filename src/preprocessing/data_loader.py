"""
数据加载器模块
负责从不同数据源加载数据
"""
import pandas as pd
import os
from pathlib import Path


class DataLoader:
    """数据加载类"""
    
    def __init__(self, data_dir="./data"):
        self.data_dir = Path(data_dir)
        
    def load_csv(self, filename, **kwargs):
        """加载 CSV 文件"""
        file_path = self.data_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在：{file_path}")
        return pd.read_csv(file_path, **kwargs)
    
    def load_excel(self, filename, **kwargs):
        """加载 Excel 文件"""
        file_path = self.data_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在：{file_path}")
        return pd.read_excel(file_path, **kwargs)
    
    def load_from_db(self, query, connection_string):
        """从数据库加载数据"""
        from sqlalchemy import create_engine
        engine = create_engine(connection_string)
        return pd.read_sql_query(query, engine)
    
    def save_processed(self, df, filename):
        """保存处理后的数据"""
        output_dir = self.data_dir / "processed"
        output_dir.mkdir(exist_ok=True)
        file_path = output_dir / filename
        df.to_csv(file_path, index=False)
        print(f"数据已保存到：{file_path}")
