"""
Checkpoint 管理器：保存/加载处理进度与聚合结果。
原子写入，支持 JSON 序列化 numpy/pandas 类型。
"""
import os
import json
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class CheckpointManager:
    """checkpoint 管理器：保存/加载处理进度与聚合结果"""

    def __init__(self, work_dir):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.work_dir / 'checkpoint.json'

    def save(self, state: dict):
        """序列化并保存 checkpoint（原子写入）"""
        state['_saved_at'] = datetime.now().isoformat()
        state['_version'] = 3
        serialized = self._to_serializable(state)
        tmp = str(self.checkpoint_file) + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(serialized, f, ensure_ascii=False)
        os.replace(tmp, str(self.checkpoint_file))
        logger.info(f"Checkpoint 已保存: {os.path.getsize(str(self.checkpoint_file)):,} 字节")

    def load(self) -> dict:
        """加载 checkpoint，不存在或损坏返回 None"""
        if not self.checkpoint_file.exists():
            logger.info("未发现 checkpoint，从头开始")
            return None
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            logger.info(f"Checkpoint 已加载 (版本 {state.get('_version', 1)}), "
                        f"已处理 {state.get('total_bytes_downloaded', 0) / (1024**3):.2f} GB, "
                        f"已处理 {state.get('total_rows_processed', 0):,} 行")
            return state
        except Exception as e:
            logger.warning(f"加载 checkpoint 失败: {e}，将从头开始")
            return None

    def exists(self) -> bool:
        return self.checkpoint_file.exists()

    def clear(self):
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

    @staticmethod
    def _to_serializable(obj):
        """递归转换 numpy 类型为原生 Python 类型"""
        if isinstance(obj, dict):
            return {str(k): CheckpointManager._to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [CheckpointManager._to_serializable(v) for v in obj]
        elif isinstance(obj, set):
            return sorted(obj)
        elif isinstance(obj, (np.integer,)):
            return int(obj)
        elif isinstance(obj, (np.floating,)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (pd.Timestamp,)):
            return obj.isoformat()
        else:
            return obj
