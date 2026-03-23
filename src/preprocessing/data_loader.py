"""
数据加载器模块
负责从不同数据源加载数据
"""
import pandas as pd
import os
from pathlib import Path
from typing import Dict, List, Optional, Any

# 导入GitHub数据加载器
try:
    from .github_loader import GitHubDataLoader
except ImportError:
    # 如果模块导入失败，提供备用实现
    class GitHubDataLoader:
        def __init__(self, *args, **kwargs):
            raise ImportError("GitHub数据加载器不可用，请确保已安装requests和PyGithub库")


class DataLoader:
    """数据加载类"""
    
    def __init__(self, data_dir="./data"):
        self.data_dir = Path(data_dir)
        self.github_loader = None
        
    def _init_github_loader(self, token: Optional[str] = None):
        """初始化GitHub数据加载器"""
        if self.github_loader is None:
            self.github_loader = GitHubDataLoader(token=token)
        
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
    
    def load_from_github(self, owner: str, repo: str, data_types: List[str] = None, 
                        token: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        从GitHub仓库加载数据
        
        Args:
            owner: 仓库所有者
            repo: 仓库名称
            data_types: 要加载的数据类型列表
            token: GitHub个人访问令牌（可选）
            
        Returns:
            包含各种数据类型的DataFrame字典
        """
        self._init_github_loader(token)
        return self.github_loader.load_repository_data(owner, repo, data_types)
    
    def load_block_traces_data(self, token: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        专门加载 alibaba/block-traces 仓库数据（针对负载规律分析）
        
        Args:
            token: GitHub个人访问令牌（可选）
            
        Returns:
            包含各种数据类型的DataFrame字典
        """
        return self.load_from_github(
            owner="alibaba",
            repo="block-traces",
            data_types=["info", "commits", "issues", "contributors"],
            token=token
        )
    
    def save_github_data(self, data_dict: Dict[str, pd.DataFrame], 
                        output_subdir: str = "github") -> None:
        """
        保存GitHub数据到CSV文件
        
        Args:
            data_dict: 包含DataFrame的字典
            output_subdir: 输出子目录
        """
        self._init_github_loader()
        output_dir = self.data_dir / output_subdir
        self.github_loader.save_github_data(data_dict, output_dir)
    
    def save_processed(self, df, filename):
        """保存处理后的数据"""
        output_dir = self.data_dir / "processed"
        output_dir.mkdir(exist_ok=True)
        file_path = output_dir / filename
        df.to_csv(file_path, index=False)
        print(f"数据已保存到：{file_path}")