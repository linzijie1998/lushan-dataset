# 庐山现代数据集

## 项目结构

- 数据库控制: dbcontroller
- 文件写入: filewriter
  - class NCGenerator: netCDF4文件写入
  - class CSVGenerator: CSV文件写入
- 文件读取: filereader
---
## 使用手册
### 1. NetCDF4和CSV文件的写入
导入filewrite包
```python
from filewriter import NCGenerator
from filewriter import CSVGenerator
```
生成netCDF4文件
```python
obj = NCGenerator(base_dir, ip, username, pwd, db_name)
obj.generate_nc_file(instrument_name,
                     header_info_code,
                     header_info_longname,
                     header_info_unit,
                     header_info_nc_type,
                     header_info_value,
                     obs_info_code,
                     obs_info_longname,
                     obs_info_unit,
                     obs_info_nc_type,
                     is_sample)
```
生成csv文件
```python
obj = CSVGenerator(base_dir, ip, username, pwd, db_name)
obj.generate_csv_file(instrument_name,
                      header_info_value,
                      obs_info_code,
                      is_sample)
```
### 2. CSV文件读取
导入filereader包
```python
from filereader import CSVReader
```
读取csv文件
```python
reader = CSVReader(filename)
data = reader.read()
```
---
## 环境依赖
- conda 4.10.3
- Python 3.8.12 
- netcdf4 1.5.7 
- numpy 1.21.2 
- pymongo 3.12.0
- pandas 1.3.4
---
## 安装部署

### 1. 安装miniconda

64位：[Miniconda3 Windows 64-bit](https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe)

32位：[Miniconda3 Windows 32-bit](https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86.exe)
### 2. 创建Python环境
```shell
conda create -n {env_name} python=3.8
```
### 3. 安装依赖
激活环境
```shell
conda activate {env_name}
```
安装依赖
```shell
conda install netCDF4 numpy pandas pymongo
```