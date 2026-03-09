"""
示例数据生成器
用于生成模拟的应用负载数据，方便测试和演示
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_load_data(days=365, output_file="./data/raw/sample_load_data.csv"):
    """
    生成模拟的应用负载数据
    
    参数:
        days: 生成多少天的数据
        output_file: 输出文件路径
    """
    print(f"正在生成 {days} 天的模拟负载数据...")
    
    # 创建时间序列（每小时一个数据点）
    start_date = datetime(2024, 1, 1)
    timestamps = pd.date_range(start=start_date, periods=days*24, freq='H')
    
    np.random.seed(42)
    
    # 模拟 CPU 使用率
    # 基础值 + 日周期模式 + 周周期模式 + 随机噪声
    base_cpu = 50
    daily_pattern = 20 * np.sin(2 * np.pi * timestamps.hour / 24)
    weekly_pattern = 10 * np.sin(2 * np.pi * timestamps.dayofweek / 7)
    noise = np.random.normal(0, 5, len(timestamps))
    cpu_usage = base_cpu + daily_pattern + weekly_pattern + noise
    
    # 确保在合理范围内
    cpu_usage = np.clip(cpu_usage, 10, 95)
    
    # 模拟内存使用率（与 CPU 相关但略高）
    memory_usage = cpu_usage * 1.2 + np.random.normal(0, 3, len(timestamps))
    memory_usage = np.clip(memory_usage, 15, 98)
    
    # 模拟请求数量（与负载正相关）
    request_count = (cpu_usage * 100 + np.random.normal(0, 50, len(timestamps))).astype(int)
    request_count = np.maximum(request_count, 0)
    
    # 模拟响应时间（毫秒）
    response_time = 100 + (cpu_usage - 50) * 2 + np.random.normal(0, 20, len(timestamps))
    response_time = np.clip(response_time, 50, 500)
    
    # 创建 DataFrame
    df = pd.DataFrame({
        'timestamp': timestamps,
        'cpu_usage': np.round(cpu_usage, 2),
        'memory_usage': np.round(memory_usage, 2),
        'request_count': request_count,
        'response_time_ms': np.round(response_time, 1),
        'active_users': np.random.randint(100, 1000, len(timestamps)),
        'error_rate': np.round(np.random.uniform(0, 2, len(timestamps)), 3)
    })
    
    # 保存数据
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"✓ 数据已保存到：{output_file}")
    print(f"✓ 数据形状：{df.shape}")
    print(f"\n数据统计信息:")
    print(df.describe())
    
    return df


if __name__ == "__main__":
    # 生成 90 天的示例数据
    df = generate_load_data(days=90)
    print("\n前 10 行数据预览:")
    print(df.head(10))
