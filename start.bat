@echo off
chcp 65001 >nul
echo ========================================
echo   大数据分析项目 - 环境配置检查
echo ========================================
echo.

REM 检查 Python 版本
echo [1/4] 检查 Python 环境...
python --version
if errorlevel 1 (
    echo ❌ Python 未安装或未添加到 PATH
    pause
    exit /b 1
)
echo ✓ Python 环境正常
echo.

REM 检查并安装依赖
echo [2/4] 检查 Python 依赖包...
pip show pandas >nul 2>&1
if errorlevel 1 (
    echo ⚠ 缺少必要的依赖包，开始安装...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ❌ 依赖安装失败
        pause
        exit /b 1
    )
) else (
    echo ✓ 依赖包已安装
)
echo.

REM 创建必要目录
echo [3/4] 检查项目结构...
if not exist "data\raw" mkdir "data\raw"
if not exist "data\processed" mkdir "data\processed"
if not exist "notebooks" mkdir "notebooks"
echo ✓ 项目结构完整
echo.

REM 启动 Jupyter
echo [4/4] 准备启动 Jupyter Notebook...
echo.
echo ========================================
echo   环境配置完成！即将打开 Jupyter...
echo ========================================
echo.
echo 提示:
echo - 从 notebooks/01_data_exploration.ipynb 开始
echo - 按 Ctrl+C 停止 Jupyter 服务
echo.
pause

REM 启动 Jupyter
echo 正在启动 Jupyter Notebook...
jupyter notebook notebooks

pause
