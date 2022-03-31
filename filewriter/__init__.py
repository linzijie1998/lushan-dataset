# -*- coding:utf-8 -*-

# @Time : 21/11/08 PM 10:01
# @Author : LINZIJIE (linzijie1998@126.com)
# @Filename : __init__.py.py
# @Version: v1.0
# @License: GNU GENERAL PUBLIC LICENSE Version 3
# @Description: netCDF4和csv文件的存储


from dbcontroller import MyMongodb
import os
import time
import pandas as pd
import numpy as np
import netCDF4 as nc


class NCGenerator:
    def __init__(self, base_dir, ip, username, pwd, db_name, port=27017):
        """
        初始化设置

        :param base_dir:    NC文件存储根目录
        :param ip:          MongoDB数据库服务器IP地址
        :param username:    登录验证的用户名
        :param pwd:         登录验证的密码
        :param db_name:     数据库名称
        :param port:        MongoDB数据库服务器端口，默认为27017
        """
        self.mongodb = MyMongodb(ip, port)
        self.db_name = db_name
        self.username = username
        self.pwd = pwd
        self.one_day_date = []
        self.base_dir = base_dir

    @staticmethod
    def generate_filename(class01, class03, class04, station_code, data_code, manufacturer_code, data_level,
                          start_time, format_code='UFMT', is_quality_control=True, class02='MODI'):
        """
        根据参数，生成nc文件的标准文件名

        :param class01: 一级分类
        :param class02: 二级分类 默认 'MODI'
        :param class03: 三级分类
        :param class04: 三级分类
        :param station_code: 站点代码
        :param data_code: 资料代码
        :param manufacturer_code: 设备厂家代码
        :param data_level: 数据级别
        :param start_time: 起始时间
        :param format_code: 格式标识 (选)
        :param is_quality_control: 是否需要质控标识
        :return: 标准文件名
        """

        # 依次对各项数据进行连接
        filename = class01 + '_' + class02 + '_' + class03 + '_' + class04 + '_'
        filename += (station_code + '_' + data_code + '_' + manufacturer_code + '_' + data_level + '_')
        datetime_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        filename += datetime_str.replace(':', '').replace('-', '').replace(' ', '_')
        if format_code is not None:
            filename += ('_' + format_code)
        # 质控选项, 默认为TRUE
        if is_quality_control:
            filename += '_QC'
        filename += '.nc'
        return filename

    def generate_one_day_nc_file(self, instrument_name, header_info_code, header_info_longname, header_info_unit,
                                 header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                 obs_info_unit, obs_info_nc_type):
        """
        生成一天数据的nc(netCDF4)文件

        :param instrument_name:         设备名
        :param header_info_code:        文件头描述信息字段代码
        :param header_info_longname:    文件头描述信息字段中英文描述
        :param header_info_unit:        文件头描述信息字段单位
        :param header_info_nc_type:     文件头描述信息字段对应nc文件中保存的数据类型
        :param header_info_value:       文件头描述信息字段对应值
        :param obs_info_code:           观测信息字段代码
        :param obs_info_longname:       观测信息字段中英文描述
        :param obs_info_unit:           观测信息字段单位
        :param obs_info_nc_type:        观测信息字段对应nc文件中保存的数据类型
        """
        if instrument_name == 'MRD':
            start_time = self.one_day_date[0]['Datetime_301']
            end_time = self.one_day_date[-1]['Datetime_301']
        else:
            start_time = self.one_day_date[0]['Datetime']  # 一天中记录开始时间
            end_time = self.one_day_date[-1]['Datetime']  # 一天中记录结束时间
        # 根据设备名、开始时间的年和月确定保存路径："./base/instrument_name/year/month"
        # print(start_time, end_time)
        path = self.base_dir + '/' + instrument_name + '/' + str(start_time.year) + '/' + str(start_time.month) + '/'
        if not os.path.exists(path):
            os.makedirs(path)
        # 根据设备名确定每个nc文件的文件名
        if instrument_name == 'AWS':  # 六要素自动站
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'AWS', 'HY', 'LX', start_time, 'FMT')
        elif instrument_name == 'PRE':  # 雨量筒
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'PRE', 'HOBO', 'LX', start_time, 'FMT')
        elif instrument_name == 'VIS':  # 能见度仪
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'VIS', 'VAIS', 'LX', start_time, 'FMT')
        elif instrument_name == 'FSD':  # 雾滴谱资料
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'FSD', 'DMT', 'LX', start_time, 'FMT')
        elif instrument_name == 'RSD':  # 雨滴谱资料
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'RSD', 'THIE', 'LX', start_time, 'FMT')
        elif instrument_name == 'YCCL_L2':  # 云高仪二级数据, 无质控码
            filename = self.generate_filename('UPAR', 'MOBS', 'SUOB', 'LSYW', 'YCCL', 'VAIS', 'L2', start_time, 'FMT',
                                          is_quality_control=False)
        elif instrument_name == 'YCCL_L3':  # 云高仪三级数据, 无质控码
            filename = self.generate_filename('UPAR', 'MOBS', 'SUOB', 'LSYW', 'YCCL', 'VAIS', 'L3', start_time, 'FMT',
                                          is_quality_control=False)
        elif instrument_name == 'RRD_Lave':  # 微雨雷达探测资料平均数据
            filename = self.generate_filename('RADA', 'MOBS', 'SUOB', 'LSYW', 'RRD', 'METE', 'Lave', start_time, 'FMT')
        elif instrument_name == 'RRD_Lpro':  # 微雨雷达探测资料再处理数据
            filename = self.generate_filename('RADA', 'MOBS', 'SUOB', 'LSYW', 'RRD', 'METE', 'Lpro', start_time, 'FMT')
        elif instrument_name == 'RRD_Lraw':  # 微雨雷达探测资料原始数据
            filename = self.generate_filename('RADA', 'MOBS', 'SUOB', 'LSYW', 'RRD', 'METE', 'Lraw', start_time, 'FMT')
        elif instrument_name == 'MRD':  # 微波辐射计探测资料二级数据 (目前只有二级数据)
            filename = self.generate_filename('UPAR', 'MOBS', 'SUOB', 'LSYW', 'MRD', 'RAD', 'L2', start_time, 'FMT')
        elif instrument_name == 'AERM':  # 颗粒物仪
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'AERM', 'THER', 'LX', start_time, 'FMT')
        else:
            print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Invalid device name: {instrument_name}')
            return
        # 修改数据记录起始和结束时间、文件生成时间
        header_info_value[-4] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        header_info_value[-3] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        header_info_value[-2] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 按instrument_name选择生成函数
        if instrument_name in ['AWS', 'AERM', 'VIS', 'YCCL_L3']:  # 观测要素只有'Datetime'一个维度的设备数据
            self.generate_one_day_one_dim_nc_file(header_info_code, header_info_longname, header_info_unit,
                                                  header_info_nc_type, header_info_value, obs_info_code,
                                                  obs_info_longname, obs_info_unit, obs_info_nc_type,
                                                  path + filename, instrument_name)
        # 以下为需要单独设置文件结构的设备
        elif instrument_name == 'RSD':
            self.generate_one_day_rsd_nc_file(header_info_code, header_info_longname, header_info_unit,
                                              header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                              obs_info_unit, obs_info_nc_type, path + filename)
        elif instrument_name == 'MRD':
            self.generate_one_day_mrd_nc_file(header_info_code, header_info_longname, header_info_unit,
                                              header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                              obs_info_unit, obs_info_nc_type, path + filename)
        elif instrument_name == 'YCCL_L2':
            self.generate_one_day_yccl_l2_nc_file(header_info_code, header_info_longname, header_info_unit,
                                                  header_info_nc_type, header_info_value, obs_info_code,
                                                  obs_info_longname, obs_info_unit, obs_info_nc_type, path + filename)
        elif instrument_name == 'RRD_Lave':
            self.generate_one_day_rrd_lave_nc_file(header_info_code, header_info_longname, header_info_unit,
                                                   header_info_nc_type, header_info_value, obs_info_code,
                                                   obs_info_longname, obs_info_unit, obs_info_nc_type, path + filename,
                                                   'Lave')
        elif instrument_name == 'RRD_Lraw':
            self.generate_one_day_rrd_lraw_nc_file(header_info_code, header_info_longname, header_info_unit,
                                                   header_info_nc_type, header_info_value, obs_info_code,
                                                   obs_info_longname, obs_info_unit, obs_info_nc_type, path + filename)
        elif instrument_name == 'RRD_Lpro':
            self.generate_one_day_rrd_lave_nc_file(header_info_code, header_info_longname, header_info_unit,
                                                   header_info_nc_type, header_info_value, obs_info_code,
                                                   obs_info_longname, obs_info_unit, obs_info_nc_type, path + filename,
                                                   'Lpro')
        elif instrument_name == 'FSD':
            self.generate_one_day_fsd_nc_file(header_info_code, header_info_longname, header_info_unit,
                                              header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                              obs_info_unit, obs_info_nc_type, path + filename)
        elif instrument_name == 'PRE':
            self.generate_one_day_pre_nc_file(header_info_code, header_info_longname, header_info_unit,
                                              header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                              obs_info_unit, obs_info_nc_type, path + filename)
        else:
            print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] '
                  f'Have no custom generate function for this device: {instrument_name}')
            return

    def generate_one_day_pre_nc_file(self, header_info_code, header_info_longname, header_info_unit,
                                     header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                     obs_info_unit, obs_info_nc_type, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating PRE\'s '
              f'nc(netCDF4) file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        nc_obj = nc.Dataset(save_path, 'w', 'NETCDF4')
        # -------------------------------------------------头文件信息组------------------------------------------------- #
        # 头文件信息组中包含三个组 (站点信息组、 设备信息组以及数据参数组)
        file_info_group = nc_obj.createGroup('file_information')
        station_group = file_info_group.createGroup('station')
        instrument_group = file_info_group.createGroup('instrument')
        data_group = file_info_group.createGroup('data')
        # 0 ~ 11: 站点信息组数据位置固定
        # 12 ~ 15: 设备信息组数据位置
        # 15之后: 参数信息组数据位置
        for i in range(len(header_info_code)):
            if i <= 11:  # 站点信息组
                var = station_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            elif 12 <= i <= 15:  # 设备信息组数据位置
                var = instrument_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            else:  # 参数信息组
                var = data_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            # 数据类型转换
            val = header_info_value[i]
            if header_info_nc_type[i] == 'f':
                val = float(val)
            elif header_info_nc_type[i] == 'i':
                val = int(val)
            else:
                val = np.array([val], dtype='object')
            # 数据存储
            var[:] = val
            var.long_name = header_info_longname[i]
            var.units = header_info_unit[i]
        # --------------------------------------------------观测要素组-------------------------------------------------- #
        obs_group = nc_obj.createGroup('observational_information')
        tem_list = []
        pre_list = []
        tem_max_pre_cum = []
        for i in range(len(self.one_day_date)):
            if self.one_day_date[i]['TEM'] != 'Nan':
                tem_list.append(self.one_day_date[i])
            if self.one_day_date[i]['PRE'] != 'Nan':
                pre_list.append(self.one_day_date[i])
            if self.one_day_date[i]['TEM_Max'] != 'Nan' or self.one_day_date[i]['PRE_Cum'] != 'Nan':
                tem_max_pre_cum.append(self.one_day_date[i])
        # 资料时间
        obs_group.createDimension('Datetime', len(self.one_day_date))
        var = obs_group.createVariable('Datetime', 'str', ('Datetime',))
        var[:] = np.array([d['Datetime'].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date])
        var.long_name = 'Datetime'
        var.units = 'yyyy-mm-dd hh:mm:ss'

        # 温度时间：
        if len(tem_list) != 0:
            obs_group.createDimension('Datetime_Temp', len(tem_list))
            var = obs_group.createVariable('Datetime_Temp', 'str', ('Datetime_Temp',))
            var[:] = np.array([d['Datetime'].strftime("%Y-%m-%d %H:%M:%S") for d in tem_list])
            var.long_name = 'Datetime of temperature'
            var.units = 'yyyy-mm-dd hh:mm:ss'

            var = obs_group.createVariable('Temp', 'f', ('Datetime_Temp',))
            var[:] = np.array([float(d['TEM']) for d in tem_list])
            var.long_name = 'Temperature'
            var.units = '°C'

            var = obs_group.createVariable('Q_Temp', 'u1', ('Datetime_Temp',))
            val = np.array([str(d['Q_TEM']) for d in tem_list])
            var[:] = np.array([str(d['Q_TEM']) for d in tem_list])
            var.long_name = 'Quality control code of temperature'
            var.units = '-'

        # 降雨量时间
        if len(pre_list) != 0:
            obs_group.createDimension('Datetime_Prec', len(pre_list))
            var = obs_group.createVariable('Datetime_Prec', 'str', ('Datetime_Prec',))
            var[:] = np.array([d['Datetime'].strftime("%Y-%m-%d %H:%M:%S") for d in pre_list])
            var.long_name = 'Datetime of precipitation'
            var.units = 'yyyy-mm-dd hh:mm:ss'

            var = obs_group.createVariable('Prec', 'f', ('Datetime_Prec',))
            var[:] = np.array([float(d['PRE']) for d in pre_list])
            var.long_name = 'Precipitation'
            var.units = 'mm'

            var = obs_group.createVariable('Q_Prec', 'u1', ('Datetime_Prec',))
            var[:] = np.array([str(d['Q_PRE']) for d in pre_list])
            var.long_name = 'Quality control code of precipitation'
            var.units = '-'

        # 整点时间
        if len(tem_max_pre_cum) != 0:
            obs_group.createDimension('Datetime_oclock', len(tem_max_pre_cum))
            var = obs_group.createVariable('Datetime_oclock', 'str', ('Datetime_oclock',))
            var[:] = np.array([d['Datetime'].strftime("%Y-%m-%d %H:%M:%S") for d in tem_max_pre_cum])
            var.long_name = 'Date and o\'clock of temperature maximum and cumulative precipitation'
            var.units = 'yyyy-mm-dd hh:mm:ss'
            var = obs_group.createVariable('Temp_MAX', 'f', ('Datetime_oclock',))
            var[:] = np.array([float(d['TEM_Max']) for d in tem_max_pre_cum])
            var.long_name = 'Temperature maximum'
            var.units = '°C'
            var = obs_group.createVariable('Q_Temp_MAX', 'u1', ('Datetime_oclock',))
            var[:] = np.array([str(d['Q_TEM_Max']) for d in tem_max_pre_cum])
            var.long_name = 'Quality control code of temperature maximum'
            var.units = '-'

            var = obs_group.createVariable('Prec_cumu', 'f', ('Datetime_oclock',))
            var[:] = np.array([float(d['PRE_Cum']) for d in tem_max_pre_cum])
            var.long_name = 'Cumulative precipitation'
            var.units = 'mm'
            var = obs_group.createVariable('Q_Prec_cumu', 'u1', ('Datetime_oclock',))
            var[:] = np.array([str(d['Q_PRE_Cum']) for d in tem_max_pre_cum])
            var.long_name = 'Quality control code of cumulative precipitation'
            var.units = '-'

        nc_obj.close()

    def generate_one_day_fsd_nc_file(self, header_info_code, header_info_longname, header_info_unit,
                                     header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                     obs_info_unit, obs_info_nc_type, save_path):
        """
        生成FSD(雾滴谱资料) nc文件的函数

        :param header_info_code:        文件头描述信息字段代码
        :param header_info_longname:    文件头描述信息字段中英文描述
        :param header_info_unit:        文件头描述信息字段单位
        :param header_info_nc_type:     文件头描述信息字段对应nc文件中保存的数据类型
        :param header_info_value:       文件头描述信息字段对应值
        :param obs_info_code:           观测信息字段代码
        :param obs_info_longname:       观测信息字段中英文描述
        :param obs_info_unit:           观测信息字段单位
        :param obs_info_nc_type:        观测信息字段对应nc文件中保存的数据类型
        :param save_path:               文件的保存路径 (路径+文件名)
        """
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating FSD\'s nc(netCDF4) '
              f'file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        nc_obj = nc.Dataset(save_path, 'w', 'NETCDF4')
        # -------------------------------------------------头文件信息组------------------------------------------------- #
        # 头文件信息组中包含三个组 (站点信息组、 设备信息组以及数据参数组)
        file_info_group = nc_obj.createGroup('file_information')
        station_group = file_info_group.createGroup('station')
        instrument_group = file_info_group.createGroup('instrument')
        data_group = file_info_group.createGroup('data')
        # 0 ~ 11: 站点信息组数据位置固定
        # 12 ~ 15: 设备信息组数据位置
        # 15之后: 参数信息组数据位置
        for i in range(len(header_info_code)):
            if i <= 11:  # 站点信息组
                var = station_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            elif 12 <= i <= 15:
                var = instrument_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            else:  # 参数信息组
                if header_info_nc_type[i] != 'array':  # 参数信息组中有 array 类型
                    var = data_group.createVariable(
                        header_info_code[i], header_info_nc_type[i], ())
                else:
                    val = np.array(header_info_value[i].split(' ')).reshape(1, -1)
                    data_group.createDimension(header_info_code[i], val.shape[-1])
                    var = data_group.createVariable(
                        header_info_code[i], 'u1', (header_info_code[i],))
                    var[:] = val
            if header_info_nc_type[i] != 'array':
                val = header_info_value[i]
                if header_info_nc_type[i] == 'f':
                    val = float(val)
                elif header_info_nc_type[i] == 'i':
                    val = int(val)
                else:
                    val = np.array([val], dtype='object')
                var[:] = val
            var.long_name = header_info_longname[i]
            var.units = header_info_unit[i]
        # --------------------------------------------------观测要素组-------------------------------------------------- #
        obs_group = nc_obj.createGroup('observational_information')
        obs_group.createDimension('Dime_numb_part_ch', 20)
        # 单独设置时间维度
        obs_group.createDimension(obs_info_code[0], len(self.one_day_date))
        var = obs_group.createVariable(
            obs_info_code[0], obs_info_nc_type[0], (obs_info_code[0],))
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        var[:] = val
        var.long_name = obs_info_longname[0]
        var.units = obs_info_unit[0]
        for v in self.one_day_date:
            if v['Volu_conc'] == 'NULL':
                v['Volu_conc'] = 'Nan'
        for i in range(1, len(obs_info_code)):
            if 'Numb_part_ch' not in obs_info_code[i]:
                var = obs_group.createVariable(
                    obs_info_code[i], obs_info_nc_type[i], (obs_info_code[0],))
                # if obs_info_nc_type[i] == 'f':
                #     val = [float(d[obs_info_code[i]]) for d in self.one_day_date]
                # elif obs_info_nc_type[i] == 'i':
                #     val = [int(d[obs_info_code[i]]) for d in self.one_day_date]
                # else:
                #     val = [str(d[obs_info_code[i]]) for d in self.one_day_date]
                val = [d[obs_info_code[i]] for d in self.one_day_date]
                var[:] = np.array(val)
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
        var = obs_group.createVariable(
            'Numb_part_chan', 'f', (obs_info_code[0], 'Dime_numb_part_ch',))
        numb_part_chan_code = [
            'Numb_part_ch0',
            'Numb_part_ch1',
            'Numb_part_ch2',
            'Numb_part_ch3',
            'Numb_part_ch4',
            'Numb_part_ch5',
            'Numb_part_ch6',
            'Numb_part_ch7',
            'Numb_part_ch8',
            'Numb_part_ch9',
            'Numb_part_ch10',
            'Numb_part_ch11',
            'Numb_part_ch12',
            'Numb_part_ch13',
            'Numb_part_ch14',
            'Numb_part_ch15',
            'Numb_part_ch16',
            'Numb_part_ch17',
            'Numb_part_ch18',
            'Numb_part_ch19'
        ]
        numb_part_chan_list = []
        for code in numb_part_chan_code:
            val = [float(d[code]) for d in self.one_day_date]
            numb_part_chan_list.append(val)
        var[:] = np.transpose(numb_part_chan_list)
        var.long_name = 'Particles number per channel'
        var.units = '-'
        nc_obj.close()
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_rrd_lraw_nc_file(self, header_info_code, header_info_longname, header_info_unit,
                                          header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                          obs_info_unit, obs_info_nc_type, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating RRD_Lraw\'s nc(netCDF4) '
              f'file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        nc_obj = nc.Dataset(save_path, 'w', 'NETCDF4')
        # -------------------------------------------------头文件信息组------------------------------------------------- #
        # 头文件信息组中包含三个组 (站点信息组、 设备信息组以及数据参数组)
        file_info_group = nc_obj.createGroup('file_information')
        station_group = file_info_group.createGroup('station')
        instrument_group = file_info_group.createGroup('instrument')
        data_group = file_info_group.createGroup('data')
        # 0 ~ 11: 站点信息组数据位置固定
        # 12 ~ 19: 设备信息组数据位置
        # 19之后: 参数信息组数据位置
        for i in range(len(header_info_code)):
            if i <= 11:  # 站点信息组
                var = station_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            elif 12 <= i <= 19:
                var = instrument_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            else:  # 参数信息组
                var = data_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            # 数据类型转换
            val = header_info_value[i]
            if header_info_nc_type[i] == 'f':
                val = float(val)
            elif header_info_nc_type[i] == 'i':
                val = int(val)
            else:
                val = np.array([val], dtype='object')
            # 数据存储
            var[:] = val
            var.long_name = header_info_longname[i]
            var.units = header_info_unit[i]
        # --------------------------------------------------观测要素组-------------------------------------------------- #
        obs_group = nc_obj.createGroup('observational_information')
        obs_group.createDimension('Dime_HGT_32', 32)
        obs_group.createDimension('Dime_part_diam_clas', 64)
        # 单独设置时间维度
        obs_group.createDimension(obs_info_code[0], len(self.one_day_date))
        var = obs_group.createVariable(
            obs_info_code[0], obs_info_nc_type[0], (obs_info_code[0],))
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        var[:] = val
        var.long_name = obs_info_longname[0]
        var.units = obs_info_unit[0]

        for i in range(1, len(obs_info_code)):
            if 'array32' not in obs_info_nc_type[i] and 'array64' not in obs_info_nc_type[i]:
                var = obs_group.createVariable(
                    obs_info_code[i], obs_info_nc_type[i], (obs_info_code[0],))
                if obs_info_nc_type[i] == 'f':
                    val = [float(d[obs_info_code[i]]) for d in self.one_day_date]
                elif obs_info_nc_type[i] == 'i':
                    val = [int(d[obs_info_code[i]]) for d in self.one_day_date]
                else:
                    val = [str(d[obs_info_code[i]]) for d in self.one_day_date]
                var[:] = np.array(val)
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
            else:
                val = [j[obs_info_code[i]] for j in self.one_day_date]
                if obs_info_nc_type[i] == 'array32_i':
                    var = obs_group.createVariable(
                        obs_info_code[i], 'u2', ('Datetime', 'Dime_HGT_32',))
                    # var[:, :] = val
                elif obs_info_nc_type[i] == 'array32_d':
                    var = obs_group.createVariable(
                        obs_info_code[i], 'd', ('Datetime', 'Dime_HGT_32',))
                    # var[:, :] = val
                else:
                    var = obs_group.createVariable(
                        obs_info_code[i], 'd', ('Datetime', 'Dime_HGT_32', 'Dime_part_diam_clas'))
                    for k in range(len(val)):
                        val[k] = np.transpose(val[k])
                var[:] = val
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
        nc_obj.close()
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_rrd_lave_nc_file(self, header_info_code, header_info_longname, header_info_unit,
                                          header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                          obs_info_unit, obs_info_nc_type, save_path, instrument_name):

        header_info_value[15] = instrument_name
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating RRD_Lave\'s nc(netCDF4) '
              f'file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        nc_obj = nc.Dataset(save_path, 'w', 'NETCDF4')
        # -------------------------------------------------头文件信息组------------------------------------------------- #
        # 头文件信息组中包含三个组 (站点信息组、 设备信息组以及数据参数组)
        file_info_group = nc_obj.createGroup('file_information')
        station_group = file_info_group.createGroup('station')
        instrument_group = file_info_group.createGroup('instrument')
        data_group = file_info_group.createGroup('data')
        # 0 ~ 11: 站点信息组数据位置固定
        # 12 ~ 18: 设备信息组数据位置
        # 18之后: 参数信息组数据位置
        for i in range(len(header_info_code)):
            if i <= 11:  # 站点信息组
                var = station_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            elif 12 <= i <= 18:
                var = instrument_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            else:  # 参数信息组
                var = data_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            # 数据类型转换
            val = header_info_value[i]
            if header_info_nc_type[i] == 'f':
                val = float(val)
            elif header_info_nc_type[i] == 'i':
                val = int(val)
            else:
                val = np.array([val], dtype='object')
            # 数据存储
            var[:] = val
            var.long_name = header_info_longname[i]
            var.units = header_info_unit[i]
        # --------------------------------------------------观测要素组-------------------------------------------------- #
        obs_group = nc_obj.createGroup('observational_information')
        obs_group.createDimension('Dime_HGT_31', 31)
        obs_group.createDimension('Dime_part_diam_clas', 64)
        # 单独设置时间维度
        obs_group.createDimension(obs_info_code[0], len(self.one_day_date))
        var = obs_group.createVariable(
            obs_info_code[0], obs_info_nc_type[0], (obs_info_code[0],))
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        var[:] = val
        var.long_name = obs_info_longname[0]
        var.units = obs_info_unit[0]

        for i in range(1, len(obs_info_code)):
            if 'array31' not in obs_info_nc_type[i] and 'array64' not in obs_info_nc_type[i]:
                var = obs_group.createVariable(
                    obs_info_code[i], obs_info_nc_type[i], (obs_info_code[0],))
                if obs_info_nc_type[i] == 'f':
                    val = [float(d[obs_info_code[i]]) for d in self.one_day_date]
                elif obs_info_nc_type[i] == 'i':
                    val = [int(d[obs_info_code[i]]) for d in self.one_day_date]
                else:
                    val = [str(d[obs_info_code[i]]) for d in self.one_day_date]
                var[:] = np.array(val)
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
            else:
                val = [j[obs_info_code[i]] for j in self.one_day_date]
                if obs_info_nc_type[i] == 'array31_i':
                    var = obs_group.createVariable(
                        obs_info_code[i], 'u2', ('Datetime', 'Dime_HGT_31',))
                elif obs_info_nc_type[i] == 'array31_f':
                    var = obs_group.createVariable(
                        obs_info_code[i], 'd', ('Datetime', 'Dime_HGT_31',))
                else:
                    var = obs_group.createVariable(
                        obs_info_code[i], 'd', ('Datetime', 'Dime_HGT_31', 'Dime_part_diam_clas'))
                    for k in range(len(val)):
                        val[k] = np.transpose(val[k])
                var[:] = val
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
        nc_obj.close()
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_yccl_l2_nc_file(self, header_info_code, header_info_longname, header_info_unit,
                                         header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                         obs_info_unit, obs_info_nc_type, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating YCCL_L2\'s nc(netCDF4) '
              f'file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        nc_obj = nc.Dataset(save_path, 'w', 'NETCDF4')
        # -------------------------------------------------头文件信息组------------------------------------------------- #
        # 头文件信息组中包含三个组 (站点信息组、 设备信息组以及数据参数组)
        file_info_group = nc_obj.createGroup('file_information')
        station_group = file_info_group.createGroup('station')
        instrument_group = file_info_group.createGroup('instrument')
        data_group = file_info_group.createGroup('data')
        # 0 ~ 11: 站点信息组数据位置固定
        # 12 ~ 15: 设备信息组数据位置
        # 15之后: 参数信息组数据位置
        for i in range(len(header_info_code)):
            if i <= 11:  # 站点信息组
                var = station_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            elif 12 <= i <= 15:
                var = instrument_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            else:  # 参数信息组
                var = data_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            # 数据类型转换
            val = header_info_value[i]
            if header_info_nc_type[i] == 'f':
                val = float(val)
            elif header_info_nc_type[i] == 'i':
                val = int(val)
            else:
                val = np.array([val], dtype='object')
            # 数据存储
            var[:] = val
            var.long_name = header_info_longname[i]
            var.units = header_info_unit[i]
        # --------------------------------------------------观测要素组-------------------------------------------------- #
        obs_group = nc_obj.createGroup('observational_information')
        obs_group.createDimension('Dime_bs_prof', 450)
        # 单独设置时间维度
        obs_group.createDimension(obs_info_code[0], len(self.one_day_date))
        var = obs_group.createVariable(
            obs_info_code[0], obs_info_nc_type[0], (obs_info_code[0],))
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        var[:] = val
        var.long_name = obs_info_longname[0]
        var.units = obs_info_unit[0]

        for i in range(1, len(obs_info_code)):
            if obs_info_nc_type[i] != 'array':
                var = obs_group.createVariable(
                    obs_info_code[i], obs_info_nc_type[i], (obs_info_code[0],))
                if obs_info_nc_type[i] == 'f':
                    val = [float(d[obs_info_code[i]]) for d in self.one_day_date]
                elif obs_info_nc_type[i] == 'i':
                    val = [int(d[obs_info_code[i]]) for d in self.one_day_date]
                else:
                    val = [str(d[obs_info_code[i]]) for d in self.one_day_date]
                var[:] = np.array(val)
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
            else:
                val = [j[obs_info_code[i]] for j in self.one_day_date]
                var = obs_group.createVariable(
                    obs_info_code[i], 'u4', ('Datetime', 'Dime_bs_prof',))
                var[:] = val
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
        nc_obj.close()
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_mrd_nc_file(self, header_info_code, header_info_longname, header_info_unit,
                                     header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                     obs_info_unit, obs_info_nc_type, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating MRD\'s nc(netCDF4) '
              f'file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        nc_obj = nc.Dataset(save_path, 'w', 'NETCDF4')
        # -------------------------------------------------头文件信息组------------------------------------------------- #
        # 头文件信息组中包含三个组 (站点信息组、 设备信息组以及数据参数组)
        file_info_group = nc_obj.createGroup('file_information')
        station_group = file_info_group.createGroup('station')
        instrument_group = file_info_group.createGroup('instrument')
        data_group = file_info_group.createGroup('data')
        # 0 ~ 11: 站点信息组数据位置固定
        # 12 ~ 15: 设备信息组数据位置
        # 15之后: 参数信息组数据位置
        for i in range(len(header_info_code)):
            if i <= 11:  # 站点信息组
                var = station_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            elif 12 <= i <= 15:  # 设备信息组位置
                var = instrument_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            else:  # 参数信息组
                if header_info_nc_type[i] != 'array':  # 参数信息组中有 array 类型
                    var = data_group.createVariable(
                        header_info_code[i], header_info_nc_type[i], ())
                else:
                    val = np.array(header_info_value[i].split(' ')).reshape(1, -1)
                    data_group.createDimension(header_info_code[i], val.shape[-1])
                    var = data_group.createVariable(
                        header_info_code[i], 'f', (header_info_code[i],))
                    var[:] = val
            if header_info_nc_type[i] != 'array':
                val = header_info_value[i]
                if header_info_nc_type[i] == 'f':
                    val = float(val)
                elif header_info_nc_type[i] == 'i':
                    val = int(val)
                else:
                    val = np.array([val], dtype='object')
                var[:] = val
            var.long_name = header_info_longname[i]
            var.units = header_info_unit[i]
        # --------------------------------------------------观测要素组-------------------------------------------------- #
        obs_group = nc_obj.createGroup('observational_information')
        obs_group.createDimension('Dime_HGT_58', 58)
        # 单独设置时间维度
        obs_group.createDimension(obs_info_code[0], len(self.one_day_date))
        var = obs_group.createVariable(
            obs_info_code[0], obs_info_nc_type[0], (obs_info_code[0],))
        date_val = []
        for date in self.one_day_date:
            if date[obs_info_code[0]] == 'Nan':
                date_val.append(date[obs_info_code[0]])
            else:
                date_val.append(date[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S"))
        val = np.array(date_val)
        # val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        var[:] = val
        var.long_name = obs_info_longname[0]
        var.units = obs_info_unit[0]

        for i in range(1, len(obs_info_code)):
            if obs_info_nc_type[i] != 'array':
                var = obs_group.createVariable(
                    obs_info_code[i], obs_info_nc_type[i], (obs_info_code[0],))
                # if obs_info_nc_type[i] == 'f':
                #     val = [float(d[obs_info_code[i]]) for d in self.one_day_date]
                # elif obs_info_nc_type[i] == 'i':
                #     val = [int(d[obs_info_code[i]]) for d in self.one_day_date]
                # else:
                #     val = [str(d[obs_info_code[i]]) for d in self.one_day_date]
                if obs_info_code[i] == 'Datetime_31' or obs_info_code[i] == 'Datetime_201' or \
                        obs_info_code[i] == 'Datetime_301' or obs_info_code[i] == 'Datetime_401' or \
                        obs_info_code[i] == 'Datetime_402' or obs_info_code[i] == 'Datetime_403' or \
                        obs_info_code[i] == 'Datetime_404' or obs_info_code[i] == 'GPS_DT':
                    dt = []
                    for d in self.one_day_date:
                        if d[obs_info_code[i]] != 'Nan':
                            dt.append(d[obs_info_code[i]].strftime("%Y-%m-%d %H:%M:%S"))
                        else:
                            dt.append('Nan')
                    val = np.array(dt)
                else:
                    val = np.array([d[obs_info_code[i]] for d in self.one_day_date])
                var[:] = val
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
            else:
                val = [j[obs_info_code[i]] for j in self.one_day_date]
                var = obs_group.createVariable(
                    obs_info_code[i], 'd', ('Datetime', 'Dime_HGT_58',))
                var[:] = val
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
        nc_obj.close()
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_rsd_nc_file(self, header_info_code, header_info_longname, header_info_unit,
                                     header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                     obs_info_unit, obs_info_nc_type, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating RSD\'s nc(netCDF4) '
              f'file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        nc_obj = nc.Dataset(save_path, 'w', 'NETCDF4')
        # -------------------------------------------------头文件信息组------------------------------------------------- #
        # 头文件信息组中包含三个组 (站点信息组、 设备信息组以及数据参数组)
        file_info_group = nc_obj.createGroup('file_information')
        station_group = file_info_group.createGroup('station')
        instrument_group = file_info_group.createGroup('instrument')
        data_group = file_info_group.createGroup('data')
        # 0 ~ 11: 站点信息组数据位置固定
        # 12 ~ 15: 设备信息组数据位置
        # 15之后: 参数信息组数据位置
        for i in range(len(header_info_code)):
            if i <= 11:  # 站点信息组
                var = station_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            elif 12 <= i <= 15:  # 设备信息组位置
                var = instrument_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            else:  # 参数信息组
                if header_info_nc_type[i] != 'array':  # 参数信息组中有 array 类型
                    var = data_group.createVariable(
                        header_info_code[i], header_info_nc_type[i], ())
                else:
                    val = np.array(header_info_value[i].split(' ')).reshape(1, -1)
                    data_group.createDimension(header_info_code[i], val.shape[-1])
                    var = data_group.createVariable(
                        header_info_code[i], 'str', (header_info_code[i],))
                    var[:] = val
            if header_info_nc_type[i] != 'array':
                val = header_info_value[i]
                if header_info_nc_type[i] == 'f':
                    val = float(val)
                elif header_info_nc_type[i] == 'i':
                    val = int(val)
                else:
                    val = np.array([val], dtype='object')
                var[:] = val
            var.long_name = header_info_longname[i]
            var.units = header_info_unit[i]
        # --------------------------------------------------观测要素组-------------------------------------------------- #
        obs_group = nc_obj.createGroup('observational_information')
        obs_group.createDimension('Dime_numb_part_diam_clas', 22)
        obs_group.createDimension('Dime_numb_part_velo_clas', 20)
        # 单独设置时间维度
        obs_group.createDimension(obs_info_code[0], len(self.one_day_date))
        var = obs_group.createVariable(
            obs_info_code[0], obs_info_nc_type[0], (obs_info_code[0],))
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        var[:] = val
        var.long_name = obs_info_longname[0]
        var.units = obs_info_unit[0]
        for i in range(1, len(obs_info_code)):
            if obs_info_nc_type[i] != 'array':
                var = obs_group.createVariable(
                    obs_info_code[i], obs_info_nc_type[i], (obs_info_code[0],))
                # if obs_info_nc_type[i] == 'f':
                #     val = [float(d[obs_info_code[i]]) for d in self.one_day_date]
                # elif obs_info_nc_type[i] == 'i':
                #     val = [int(d[obs_info_code[i]]) for d in self.one_day_date]
                # else:
                #     val = [str(d[obs_info_code[i]]) for d in self.one_day_date]
                # print(obs_info_code[i], obs_info_nc_type[i])
                if obs_info_code[i] == 'Syno_4678_1MIN' or obs_info_code[i] == 'Syno_4678_5MIN':
                    val = [str(d[obs_info_code[i]]).replace(' ', '') for d in self.one_day_date]
                else:
                    val = [d[obs_info_code[i]] for d in self.one_day_date]
                var[:] = np.array(val)
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
            else:
                val = [j[obs_info_code[i]] for j in self.one_day_date]
                var = obs_group.createVariable(
                    obs_info_code[i], 'u2', ('Datetime', 'Dime_numb_part_diam_clas', 'Dime_numb_part_velo_clas',))
                val = np.array(val, dtype='object')
                values = []
                for v in val:
                    if len(v) != 440:
                        tmp = []
                        for d in v:
                            if len(d) != 3:
                                tmp.append(d[:3])
                                tmp.append(d[4:])
                            else:
                                tmp.append(d)
                        values.append(np.array(tmp).reshape(22, 20))
                    else:
                        values.append(np.array(v).reshape(22, 20))
                val = np.array(values)
                var[:, :] = np.array(val, dtype='object')
                var.long_name = obs_info_longname[i]
                var.units = obs_info_unit[i]
        nc_obj.close()
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_one_dim_nc_file(self, header_info_code, header_info_longname, header_info_unit,
                                         header_info_nc_type, header_info_value, obs_info_code, obs_info_longname,
                                         obs_info_unit, obs_info_nc_type, save_path, instrument_name):
        """
        数据维度

        :param header_info_code:
        :param header_info_longname:
        :param header_info_unit:
        :param header_info_nc_type:
        :param header_info_value:
        :param obs_info_code:
        :param obs_info_longname:
        :param obs_info_unit:
        :param obs_info_nc_type:
        :param save_path:
        :param instrument_name:
        :return:
        """
        if instrument_name == 'AERM':
            my_datetime = np.array([d[obs_info_code[0]] for d in self.one_day_date])
            for i in range(1, len(my_datetime)):
                tmp = (my_datetime[i] - my_datetime[i - 1]).seconds
                if tmp <= 10:
                    header_info_value[18] = 6
                    break
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating {instrument_name}\'s '
              f'nc(netCDF4) file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        nc_obj = nc.Dataset(save_path, 'w', 'NETCDF4')
        # -------------------------------------------------头文件信息组------------------------------------------------- #
        # 头文件信息组中包含三个组 (站点信息组、 设备信息组以及数据参数组)
        file_info_group = nc_obj.createGroup('file_information')
        station_group = file_info_group.createGroup('station')
        instrument_group = file_info_group.createGroup('instrument')
        data_group = file_info_group.createGroup('data')
        # 0 ~ 11: 站点信息组数据位置固定
        # 12 ~ index_dict[instrument_name]: 设备信息组数据位置
        # index_dict[instrument_name]之后: 参数信息组数据位置
        index_dict = {'VIS': 15, 'AERM': 15, 'AWS': 17, 'YCCL_L3': 15}
        for i in range(len(header_info_code)):
            if i <= 11:  # 站点信息组
                var = station_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            elif 12 <= i <= index_dict[instrument_name]:  # 根据字典确定设备信息组数据位置
                var = instrument_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            else:  # 参数信息组
                var = data_group.createVariable(
                    header_info_code[i], header_info_nc_type[i], ())
            # 数据类型转换
            val = header_info_value[i]
            if header_info_nc_type[i] == 'f':
                val = float(val)
            elif header_info_nc_type[i] == 'i':
                val = int(val)
            else:
                val = np.array([val], dtype='object')
            # 数据存储
            var[:] = val
            var.long_name = header_info_longname[i]
            var.units = header_info_unit[i]

        # --------------------------------------------------观测要素组-------------------------------------------------- #
        obs_group = nc_obj.createGroup('observational_information')
        # 单独设置时间维度
        obs_group.createDimension(obs_info_code[0], len(self.one_day_date))
        var = obs_group.createVariable(
            obs_info_code[0], obs_info_nc_type[0], (obs_info_code[0],))
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        var[:] = val
        var.long_name = obs_info_longname[0]
        var.units = obs_info_unit[0]
        # 其他数据
        for i in range(1, len(obs_info_code)):
            var = obs_group.createVariable(
                obs_info_code[i], obs_info_nc_type[i], (obs_info_code[0],))
            # if obs_info_nc_type[i] == 'f':
            #     val = [float(d[obs_info_code[i]]) for d in self.one_day_date]
            # elif obs_info_nc_type[i] == 'u4' or obs_info_code[i] == 'u1':
            #     val = [int(d[obs_info_code[i]]) for d in self.one_day_date]
            # else:
            #     val = [str(d[obs_info_code[i]]) for d in self.one_day_date]
            # print(obs_info_code[i], obs_info_nc_type[i])
            val = [d[obs_info_code[i]] for d in self.one_day_date]
            var[:] = np.array(val)
            var.long_name = obs_info_longname[i]
            var.units = obs_info_unit[i]

        nc_obj.close()
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_nc_file(self, instrument_name, header_info_code, header_info_longname, header_info_unit,
                         header_info_nc_type, header_info_value, obs_info_code, obs_info_longname, obs_info_unit,
                         obs_info_nc_type, is_sample=False):
        """
        根据设备名、文件头信息、观测信息生成nc(netCDF4)文件.
        该函数is_sample参数能够控制nc文件的生成模式 (样例模式和生成模式).
        当字段与数据库不匹配时会抛出异常.

        :param instrument_name:         设备名
        :param header_info_code:        文件头描述信息字段代码
        :param header_info_longname:    文件头描述信息字段中英文描述
        :param header_info_unit:        文件头描述信息字段单位
        :param header_info_nc_type:     文件头描述信息字段对应nc文件中保存的数据类型
        :param header_info_value:       文件头描述信息字段对应值
        :param obs_info_code:           观测信息字段代码
        :param obs_info_longname:       观测信息字段中英文描述
        :param obs_info_unit:           观测信息字段单位
        :param obs_info_nc_type:        观测信息字段对应nc文件中保存的数据类型
        :param is_sample:               是否属于样例文件生成模式， 默认为False
        """
        # 以下设备数据的MongoDB集合后缀为 "_VQ1"
        if instrument_name in ['']:
            db_data = self.mongodb.get_collection_data(self.db_name, instrument_name + '_VQ1', self.username, self.pwd)
        else:
            db_data = self.mongodb.get_collection_data(self.db_name, instrument_name + '_VQ', self.username, self.pwd)
        date_flag = ''  # 每条记录的时间标识，初始为空
        one_day_data_list = []  # 保存一天数据的 list

        sample_size = 3
        count = 0

        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Loading {instrument_name}\'s data...')
        for data in db_data:  # 对数据库返回来的数据进行迭代
            filtered_data = {}  # 过滤出需要的字段
            for c in obs_info_code:
                filtered_data[c] = data[c]
            if instrument_name == 'MRD':
                now_data_date = filtered_data['Datetime_301'].strftime("%Y-%m-%d")
            else:
                now_data_date = filtered_data['Datetime'].strftime("%Y-%m-%d")  # 每读完一条更新当前时间信息
            if date_flag == '':  # 第一条数据是更新 date_flag
                print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Found start time: {now_data_date}')
                date_flag = now_data_date
            if date_flag != now_data_date:  # 获取一天数据完成
                self.one_day_date = one_day_data_list
                self.generate_one_day_nc_file(instrument_name, header_info_code, header_info_longname,
                                              header_info_unit, header_info_nc_type, header_info_value,
                                              obs_info_code, obs_info_longname, obs_info_unit, obs_info_nc_type)
                count += 1
                # 生成样例文件只生成一个文件即可
                if is_sample:
                    if count >= sample_size:
                        db_data.close()
                        return
                one_day_data_list.clear()  # 一天数据生成后清空
                date_flag = now_data_date  # 更新当前时间
                print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Found start time: {now_data_date}')
                one_day_data_list.append(filtered_data)  # 添加新一天的第一条数据
            else:  # 未获取完一天数据
                one_day_data_list.append(filtered_data)
        # 保存数据库中最后一天的数据
        if len(one_day_data_list) != 0:
            self.one_day_date = one_day_data_list
            self.generate_one_day_nc_file(instrument_name, header_info_code, header_info_longname,
                                          header_info_unit, header_info_nc_type, header_info_value,
                                          obs_info_code, obs_info_longname, obs_info_unit, obs_info_nc_type)
        db_data.close()


class CSVGenerator:
    def __init__(self, base_dir, ip, username, pwd, db_name):
        self.mongodb = MyMongodb(ip)    # 连接MongoDB
        self.db_name = db_name          # 数据库名称
        self.username = username        # 用户名
        self.pwd = pwd                  # 密码
        self.one_day_date = []          # 一天的数据
        self.base_dir = base_dir        # CSV文件存储根目录

    @staticmethod
    def generate_filename(class01, class03, class04, station_code, data_code, manufacturer_code, data_level,
                          start_time, format_code='UFMT', is_quality_control=True, class02='MODI'):
        """
        根据参数，生成nc文件的标准文件名

        :param class01: 一级分类
        :param class02: 二级分类 默认 'MODI'
        :param class03: 三级分类
        :param class04: 三级分类
        :param station_code: 站点代码
        :param data_code: 资料代码
        :param manufacturer_code: 设备厂家代码
        :param data_level: 数据级别
        :param start_time: 起始时间
        :param format_code: 格式标识 (选)
        :param is_quality_control: 是否需要质控标识
        :return: 标准文件名
        """

        # 依次对各项数据进行连接
        filename = class01 + '_' + class02 + '_' + class03 + '_' + class04 + '_'
        filename += (station_code + '_' + data_code + '_' + manufacturer_code + '_' + data_level + '_')
        datetime_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        filename += datetime_str.replace(':', '').replace('-', '').replace(' ', '_')
        if format_code is not None:
            filename += ('_' + format_code)
        # 质控选项, 默认为TRUE
        if is_quality_control:
            filename += '_QC'
        filename += '.csv'
        return filename

    def generate_csv_file(self, instrument_name, header_info_value, obs_info_code, is_sample=False):
        """
        根据设备名、文件头信息、观测信息生成csv文件.
        该函数is_sample参数能够控制nc文件的生成模式 (样例模式和生成模式).
        当字段与数据库不匹配时会抛出异常.

        :param instrument_name:         设备名
        :param header_info_value:       文件头描述信息字段对应值
        :param obs_info_code:           观测信息字段代码
        :param is_sample:               是否属于样例文件生成模式， 默认为False
        """

        db_data = self.mongodb.get_collection_data(self.db_name, instrument_name + '_VQ', self.username, self.pwd)
        date_flag = ''  # 每条记录的时间标识，初始为空
        one_day_data_list = []  # 保存一天数据的 list
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Loading {instrument_name}\'s data...')
        for data in db_data:  # 对数据库返回来的数据进行迭代
            filtered_data = {}  # 过滤出需要的字段
            for c in obs_info_code:
                filtered_data[c] = data[c]
            if instrument_name == 'MRD':
                now_data_date = filtered_data['Datetime_301'].strftime("%Y-%m-%d")
            else:
                now_data_date = filtered_data['Datetime'].strftime("%Y-%m-%d")
            # now_data_date = filtered_data['Datetime'].strftime("%Y-%m-%d")  # 每读完一条更新当前时间信息
            if date_flag == '':  # 第一条数据是更新 date_flag
                print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Found start time: {now_data_date}')
                date_flag = now_data_date
            if date_flag != now_data_date:  # 获取一天数据完成
                self.one_day_date = one_day_data_list
                self.generate_one_day_csv_file(instrument_name, header_info_value, obs_info_code)
                # 生成样例文件只生成一个文件即可
                if is_sample:
                    db_data.close()
                    return
                one_day_data_list.clear()  # 一天数据生成后清空
                date_flag = now_data_date  # 更新当前时间
                print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Found start time: {now_data_date}')
                one_day_data_list.append(filtered_data)  # 添加新一天的第一条数据
            else:  # 未获取完一天数据
                one_day_data_list.append(filtered_data)
        # 保存数据库中最后一天的数据
        if len(one_day_data_list) != 0:
            self.one_day_date = one_day_data_list
            self.generate_one_day_csv_file(instrument_name, header_info_value, obs_info_code)
        db_data.close()

    def generate_one_day_csv_file(self, instrument_name, header_info_value, obs_info_code):
        """
        生成一天数据的nc(netCDF4)文件

        :param instrument_name:         设备名
        :param header_info_value:       文件头描述信息字段对应值
        :param obs_info_code:           观测信息字段代码
        """
        if instrument_name == 'MRD':
            start_time = self.one_day_date[0]['Datetime_301']
            end_time = self.one_day_date[-1]['Datetime_301']
        else:
            start_time = self.one_day_date[0]['Datetime']  # 一天中记录开始时间
            end_time = self.one_day_date[-1]['Datetime']  # 一天中记录结束时间
        # start_time = self.one_day_date[0]['Datetime']  # 一天中记录开始时间
        # end_time = self.one_day_date[-1]['Datetime']  # 一天中记录结束时间
        # 根据设备名、开始时间的年和月确定保存路径："./base/instrument_name/year/month"
        path = self.base_dir + '/' + instrument_name + '/' + str(start_time.year) + '/' + str(
            start_time.month) + '/'
        # 根据设备名确定每个nc文件的文件名
        if instrument_name == 'AWS':        # 六要素自动站
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'AWS', 'HY', 'LX',
                                              start_time)
        elif instrument_name == 'PRE':      # 雨量筒
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'PRE', 'HOBO', 'LX',
                                              start_time)
        elif instrument_name == 'VIS':      # 能见度仪
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'VIS', 'VAIS', 'LX',
                                              start_time)
        elif instrument_name == 'FSD':      # 雾滴谱资料
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'FSD', 'DMT', 'LX',
                                              start_time)
        elif instrument_name == 'RSD':      # 雨滴谱资料
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'RSD', 'THIE', 'LX',
                                              start_time)
        elif instrument_name == 'YCCL_L2':  # 云高仪二级数据
            filename = self.generate_filename('UPAR', 'MOBS', 'SUOB', 'LSYW', 'YCCL', 'VAIS', 'L2',
                                              start_time, is_quality_control=False)
        elif instrument_name == 'YCCL_L3':  # 云高仪三级数据
            filename = self.generate_filename('UPAR', 'MOBS', 'SUOB', 'LSYW', 'YCCL', 'VAIS', 'L3',
                                              start_time, is_quality_control=False)
        elif instrument_name == 'RRD_Lave':     # 微雨雷达探测资料平均数据
            header_info_value[22] = 'Lave'
            filename = self.generate_filename('RADA', 'MOBS', 'SUOB', 'LSYW', 'RRD', 'METE', 'Lave',
                                              start_time)
        elif instrument_name == 'RRD_Lpro':     # 微雨雷达探测资料再处理数据
            header_info_value[22] = 'Lpro'
            filename = self.generate_filename('RADA', 'MOBS', 'SUOB', 'LSYW', 'RRD', 'METE', 'Lpro',
                                              start_time)
        elif instrument_name == 'RRD_Lraw':     # 微雨雷达探测资料原始数据
            filename = self.generate_filename('RADA', 'MOBS', 'SUOB', 'LSYW', 'RRD', 'METE', 'Lraw',
                                              start_time)
        elif instrument_name == 'MRD':          # 微波辐射计探测资料二级数据 (目前只有二级数据)
            filename = self.generate_filename('UPAR', 'MOBS', 'SUOB', 'LSYW', 'MRD', 'RAD', 'L2',
                                              start_time)
        elif instrument_name == 'AERM':         # 颗粒物仪
            filename = self.generate_filename('SURF', 'MOBS', 'SUOB', 'LSYW', 'AERM', 'THER', 'LX',
                                              start_time)
        else:
            print(
                f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Invalid device name: {instrument_name}')
            return
        if not os.path.exists(path):
            os.makedirs(path)
        # 修改数据记录起始和结束时间、文件生成时间
        header_info_value[-4] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        header_info_value[-3] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        header_info_value[-2] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 按instrument_name选择生成函数
        if instrument_name in ['AWS', 'AERM', 'VIS', 'YCCL_L3']:  # 观测要素只有'Datetime'一个维度的设备数据
            self.generate_one_day_one_dim_csv_file(header_info_value, obs_info_code,
                                                   path + filename, instrument_name)
        elif instrument_name == 'RSD':
            self.generate_one_day_rsd_csv_file(header_info_value, obs_info_code, path + filename)
        elif instrument_name == 'MRD':
            self.generate_one_day_mrd_csv_file(header_info_value, obs_info_code, path + filename)
        elif instrument_name == 'YCCL_L2':
            self.generate_one_day_yccl_l2_csv_file(header_info_value, obs_info_code, path + filename)
        elif instrument_name == 'RRD_Lraw':
            self.generate_one_day_rrd_lraw_csv_file(header_info_value, obs_info_code, path + filename)
        elif instrument_name == 'RRD_Lave':
            self.generate_one_day_rrd_lave_csv_file(header_info_value, obs_info_code, instrument_name,
                                                    path + filename)
        elif instrument_name == 'RRD_Lpro':
            self.generate_one_day_rrd_lave_csv_file(header_info_value, obs_info_code, instrument_name,
                                                    path + filename)
        elif instrument_name == 'FSD':
            self.generate_one_day_fsd_csv_file(header_info_value, obs_info_code, path + filename)
        elif instrument_name == 'PRE':
            self.generate_one_day_pre_csv_file(header_info_value, path + filename)
        else:
            print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] '
                  f'Have no custom generate function for this device: {instrument_name}')

    def generate_one_day_one_dim_csv_file(self, header_info_value, obs_info_code, save_path,
                                          instrument_name):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating {instrument_name}\'s '
              f'csv file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        if instrument_name == 'AERM':
            my_datetime = np.array([d[obs_info_code[0]] for d in self.one_day_date])
            for i in range(1, len(my_datetime)):
                tmp = (my_datetime[i] - my_datetime[i - 1]).seconds
                if tmp <= 10:
                    header_info_value[18] = 6
                    break
        # --------------------------------------------------头文件信息-------------------------------------------------- #
        header_dataframe = pd.DataFrame([header_info_value[0]])
        for i in range(1, len(header_info_value)):
            val = pd.DataFrame([header_info_value[i]])
            header_dataframe = pd.concat([header_dataframe, val], axis=1)
        header_dataframe.to_csv(save_path, header=False, index=False, mode='w')

        # -------------------------------------------------观测要素信息------------------------------------------------- #
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        obs_dataframe = pd.DataFrame(val, columns=[obs_info_code[0]])
        for i in range(1, len(obs_info_code)):
            val = pd.DataFrame([d[obs_info_code[i]] for d in self.one_day_date], columns=[obs_info_code[i]])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
        obs_dataframe.to_csv(save_path, index=False, mode='a')
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_mrd_csv_file(self, header_info_value, obs_info_code, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating MRD\'s '
              f'csv file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        # --------------------------------------------------头文件信息-------------------------------------------------- #
        header_dataframe = pd.DataFrame([header_info_value[0]])
        for i in range(1, len(header_info_value)):
            val = pd.DataFrame([header_info_value[i]])
            header_dataframe = pd.concat([header_dataframe, val], axis=1)
        header_dataframe.to_csv(save_path, header=False, index=False, mode='w')
        # -------------------------------------------------观测要素信息------------------------------------------------- #
        # val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date], dtype='object')
        date_val = []
        for date in self.one_day_date:
            if date[obs_info_code[0]] == 'Nan':
                date_val.append(date[obs_info_code[0]])
            else:
                date_val.append(date[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S"))
        val = np.array(date_val)
        obs_dataframe = pd.DataFrame(val, columns=[obs_info_code[0]])
        for i in range(1, len(obs_info_code)):
            if obs_info_code[i] in ['Temp_prof', 'VAP_prof', 'LWC_prof', 'RH_prof']:
                col_names = [obs_info_code[i]]
                for _ in range(57):
                    col_names.append('')
                val = [d[obs_info_code[i]] for d in self.one_day_date]
                data_list = [val[0]]
                for j in range(1, len(val)):
                    data_list.append(val[j])
                data_dataframe = pd.DataFrame(data_list, columns=col_names)
                obs_dataframe = pd.concat([obs_dataframe, data_dataframe], axis=1)
            elif obs_info_code[i] == 'Datetime_31' or obs_info_code[i] == 'Datetime_201' or \
                    obs_info_code[i] == 'Datetime_301' or obs_info_code[i] == 'Datetime_401' or \
                    obs_info_code[i] == 'Datetime_402' or obs_info_code[i] == 'Datetime_403' or \
                    obs_info_code[i] == 'Datetime_404' or obs_info_code[i] == 'GPS_DT':
                dt = []
                for d in self.one_day_date:
                    if d[obs_info_code[i]] != 'Nan':
                        dt.append(d[obs_info_code[i]].strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        dt.append('Nan')
                # val = np.array(dt)
                data_dataframe = pd.DataFrame(dt, columns=[obs_info_code[i]])
                obs_dataframe = pd.concat([obs_dataframe, data_dataframe], axis=1)
            else:
                val = pd.DataFrame([d[obs_info_code[i]] for d in self.one_day_date], columns=[obs_info_code[i]])
                obs_dataframe = pd.concat([obs_dataframe, val], axis=1)

        obs_dataframe.to_csv(save_path, index=False, mode='a')
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_rsd_csv_file(self, header_info_value, obs_info_code, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating RSD\'s '
              f'csv file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        # --------------------------------------------------头文件信息-------------------------------------------------- #
        header_dataframe = pd.DataFrame([header_info_value[0]])
        for i in range(1, len(header_info_value)):
            val = pd.DataFrame([header_info_value[i]])
            header_dataframe = pd.concat([header_dataframe, val], axis=1)
        header_dataframe.to_csv(save_path, header=False, index=False, mode='w')

        # -------------------------------------------------观测要素信息------------------------------------------------- #
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date],
                       dtype='object')
        prec_list = [d['Prec_spec'] for d in self.one_day_date]
        values = []
        for v in prec_list:
            if len(v) != 440:
                tmp = []
                for d in v:
                    if len(d) != 3:
                        tmp.append(int(d[:3]))
                        tmp.append(int(d[4:]))
                    else:
                        tmp.append(int(d))
                values.append(np.array(tmp).reshape(1, 440))
            else:
                values.append(np.array([int(d) for d in v]).reshape(1, 440))
        col_names = ['Prec_spec']
        for _ in range(439):
            col_names.append('')
        for i in range(len(val)):
            one_day_dataframe = pd.DataFrame()
            for code in obs_info_code:
                if code == 'Datetime':
                    one_day_dataframe = pd.concat(
                        [one_day_dataframe, pd.DataFrame([val[i]], columns=['Datetime'])], axis=1)
                elif code == 'Prec_spec':
                    one_day_dataframe = pd.concat(
                        [one_day_dataframe, pd.DataFrame(values[i], columns=col_names)], axis=1)
                elif code == 'Syno_4678_5MIN' or code == 'Syno_4678_1MIN':
                    value = self.one_day_date[i][code]
                    if value != 'Nan':
                        value = value.replace(' ', '')
                    one_day_dataframe = pd.concat(
                        [one_day_dataframe, pd.DataFrame([value], columns=[code])], axis=1)
                else:
                    one_day_dataframe = pd.concat(
                        [one_day_dataframe, pd.DataFrame([self.one_day_date[i][code]], columns=[code])], axis=1)
            if i == 0:
                one_day_dataframe.to_csv(save_path, index=False, mode='a')
            else:
                one_day_dataframe.to_csv(save_path, index=False, header=False, mode='a')
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_yccl_l2_csv_file(self, header_info_value, obs_info_code, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating YCCL_L2\'s '
              f'csv file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        # --------------------------------------------------头文件信息-------------------------------------------------- #
        header_dataframe = pd.DataFrame([header_info_value[0]])
        for i in range(1, len(header_info_value)):
            val = pd.DataFrame([header_info_value[i]])
            header_dataframe = pd.concat([header_dataframe, val], axis=1)
        header_dataframe.to_csv(save_path, header=False, index=False, mode='w')
        # -------------------------------------------------观测要素信息------------------------------------------------- #
        date_list = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date],
                             dtype='object')
        obs_dataframe = pd.DataFrame(date_list, columns=[obs_info_code[0]])
        for i in range(1, len(obs_info_code)):
            if obs_info_code[i] == 'BS_prof':
                col_names = [obs_info_code[i]]
                for _ in range(449):
                    col_names.append('')
                val = [d[obs_info_code[i]] for d in self.one_day_date]
                data_list = [val[0]]
                for j in range(1, len(val)):
                    data_list.append(val[j])
                    # data_list = np.transpose(data_list)
                data_dataframe = pd.DataFrame(data_list, columns=col_names)
                obs_dataframe = pd.concat([obs_dataframe, data_dataframe], axis=1)
            else:
                val = pd.DataFrame([d[obs_info_code[i]] for d in self.one_day_date], columns=[obs_info_code[i]])
                obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
        obs_dataframe.to_csv(save_path, index=False, mode='a')
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_rrd_lraw_csv_file(self, header_info_value, obs_info_code, save_path, one_day_data=None):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating RRD_Lraw\'s '
              f'csv file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        if one_day_data is None:
            one_day_data = self.one_day_date
        # --------------------------------------------------头文件信息-------------------------------------------------- #
        header_dataframe = pd.DataFrame([header_info_value[0]])
        for i in range(1, len(header_info_value)):
            val = pd.DataFrame([header_info_value[i]])
            header_dataframe = pd.concat([header_dataframe, val], axis=1)
        header_dataframe.to_csv(save_path, header=False, index=False, mode='w')
        # -------------------------------------------------观测要素信息------------------------------------------------- #
        date_list = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in one_day_data],
                             dtype='object')
        for i in range(len(date_list)):
            for code in obs_info_code:
                if code == 'Datetime':
                    pd.DataFrame(np.array([code, date_list[i]]).reshape(1, -1)).to_csv(
                        save_path, mode='a', index=False, header=False)
                elif code == 'HGT' or code == 'Transfer_function':
                    val = [d[code] for d in one_day_data][i]
                    val.insert(0, code)
                    pd.DataFrame(np.array(val).reshape(1, -1)).to_csv(
                        save_path, mode='a', index=False, header=False)
                elif code == 'Spectral_reflectivities':
                    val = [d[code] for d in one_day_data][i]
                    spec = pd.concat([pd.DataFrame([code]), pd.DataFrame(val)], axis=1)
                    spec.to_csv(save_path, mode='a', index=False, header=False)
                else:
                    val = [d[code] for d in one_day_data][i]
                    pd.DataFrame(np.array([code, val]).reshape(1, -1)).to_csv(
                        save_path, mode='a', index=False, header=False)
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_rrd_lave_csv_file(self, header_info_value, obs_info_code, instrument_name,
                                           save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating {instrument_name}\'s '
              f'csv file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        # --------------------------------------------------头文件信息-------------------------------------------------- #
        header_dataframe = pd.DataFrame([header_info_value[0]])
        for i in range(1, len(header_info_value)):
            val = pd.DataFrame([header_info_value[i]])
            header_dataframe = pd.concat([header_dataframe, val], axis=1)
        header_dataframe.to_csv(save_path, header=False, index=False, mode='w')
        # -------------------------------------------------观测要素信息------------------------------------------------- #
        date_list = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date],
                             dtype='object')
        for i in range(len(date_list)):
            for code in obs_info_code:
                if code == 'Datetime':
                    pd.DataFrame(np.array([code, date_list[i]]).reshape(1, -1)).to_csv(
                        save_path, mode='a', index=False, header=False)
                elif code == 'HGT' or code == 'Transfer_function' or code == 'Path_Inte_Atte' or code == 'Z_Atte' or \
                        code == 'LWC' or code == 'W' or code == 'Z_Atte_corr' or code == 'Rain_rate':
                    val = [d[code] for d in self.one_day_date][i]
                    val.insert(0, code)
                    pd.DataFrame(np.array(val).reshape(1, -1)).to_csv(
                        save_path, mode='a', index=False, header=False)
                elif code == 'Spectral_reflectivities' or code == 'Drop_size' or code == 'Spec_drop_dens':
                    val = [d[code] for d in self.one_day_date][i]
                    spec = pd.concat([pd.DataFrame([code]), pd.DataFrame(val)], axis=1)
                    spec.to_csv(save_path, mode='a', index=False, header=False)
                else:
                    val = [d[code] for d in self.one_day_date][i]
                    pd.DataFrame(np.array([code, val]).reshape(1, -1)).to_csv(
                        save_path, mode='a', index=False, header=False)
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_fsd_csv_file(self, header_info_value, obs_info_code, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating FSD\'s '
              f'csv file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        # --------------------------------------------------头文件信息-------------------------------------------------- #
        header_dataframe = pd.DataFrame([header_info_value[0]])
        for i in range(1, len(header_info_value)):
            val = pd.DataFrame([header_info_value[i]])
            header_dataframe = pd.concat([header_dataframe, val], axis=1)
        header_dataframe.to_csv(save_path, header=False, index=False, mode='w')
        # -------------------------------------------------观测要素信息------------------------------------------------- #
        val = np.array([d[obs_info_code[0]].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date],
                       dtype='object')
        obs_dataframe = pd.DataFrame(val, columns=[obs_info_code[0]])
        for i in range(1, len(obs_info_code)):
            if 'Numb_part_ch' in obs_info_code[i]:
                if obs_info_code[i] == 'Numb_part_ch0':
                    val = pd.DataFrame([d[obs_info_code[i]] for d in self.one_day_date], columns=['Numb_part_chan'])
                else:
                    val = pd.DataFrame([d[obs_info_code[i]] for d in self.one_day_date], columns=[''])
            else:
                val = pd.DataFrame([d[obs_info_code[i]] for d in self.one_day_date], columns=[obs_info_code[i]])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
        obs_dataframe.to_csv(save_path, index=False, mode='a')
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')

    def generate_one_day_pre_csv_file(self, header_info_value, save_path):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] Generating PRE\'s '
              f'csv file: {header_info_value[-4]} ~ {header_info_value[-3]}')
        # --------------------------------------------------头文件信息-------------------------------------------------- #
        header_dataframe = pd.DataFrame([header_info_value[0]])
        for i in range(1, len(header_info_value)):
            val = pd.DataFrame([header_info_value[i]])
            header_dataframe = pd.concat([header_dataframe, val], axis=1)
        header_dataframe.to_csv(save_path, header=False, index=False, mode='w')
        # -------------------------------------------------观测要素信息------------------------------------------------- #
        tem_list = []
        pre_list = []
        tem_max_pre_cum = []
        for i in range(len(self.one_day_date)):
            if self.one_day_date[i]['TEM'] != 'Nan':
                tem_list.append(self.one_day_date[i])
            if self.one_day_date[i]['PRE'] != 'Nan':
                pre_list.append(self.one_day_date[i])
            if self.one_day_date[i]['TEM_Max'] != 'Nan' or self.one_day_date[i]['PRE_Cum'] != 'Nan':
                tem_max_pre_cum.append(self.one_day_date[i])

        # 资料时间
        # obs_dataframe = pd.DataFrame(
        #     [d['Datetime'].strftime("%Y-%m-%d %H:%M:%S") for d in self.one_day_date],
        #     columns=['Datetime'])

        # 温度时间：
        val = pd.DataFrame(
            [d['Datetime'].strftime("%Y-%m-%d %H:%M:%S") for d in tem_list], columns=['Datetime_Temp'])
        obs_dataframe = pd.concat([val], axis=1)
        # 温度
        val = pd.DataFrame([float(d['TEM']) for d in tem_list], columns=['Temp'])
        obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
        # 温度质控码
        val = pd.DataFrame([int(d['Q_TEM']) for d in tem_list], columns=['Q_Temp'])
        obs_dataframe = pd.concat([obs_dataframe, val], axis=1)

        # 降雨量时间
        if len(pre_list) != 0:
            val = pd.DataFrame(
                [d['Datetime'].strftime("%Y-%m-%d %H:%M:%S") for d in pre_list], columns=['Datetime_Prec'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            # 降雨量
            val = pd.DataFrame([float(d['PRE']) for d in pre_list], columns=['Prec'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            # 降雨量质控码
            val = pd.DataFrame([int(d['Q_PRE']) for d in pre_list], columns=['Q_Prec'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
        else:
            val = pd.DataFrame([''], columns=['Datetime_Prec'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            val = pd.DataFrame([''], columns=['Prec'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            val = pd.DataFrame([''], columns=['Q_Prec'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)

        # 整点时间
        if len(tem_max_pre_cum) != 0:
            val = pd.DataFrame(
                [d['Datetime'].strftime("%Y-%m-%d %H:%M:%S") for d in tem_max_pre_cum],
                columns=['Datetime_oclock'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            # 最大温度
            val = pd.DataFrame([float(d['TEM_Max']) for d in tem_max_pre_cum], columns=['Temp_MAX'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            # 累计降雨量
            val = pd.DataFrame([float(d['PRE_Cum']) for d in tem_max_pre_cum], columns=['Prec_cumu'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            # 最大温度质控码
            val = pd.DataFrame([int(d['Q_TEM_Max']) for d in tem_max_pre_cum], columns=['Q_Temp_MAX'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            # 累计降雨量质控码
            val = pd.DataFrame([int(d['Q_PRE_Cum']) for d in tem_max_pre_cum], columns=['Q_Prec_cumu'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
        else:
            val = pd.DataFrame([''], columns=['Datetime_oclock'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            val = pd.DataFrame([''], columns=['Temp_MAX'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            val = pd.DataFrame([''], columns=['Prec_cumu'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            val = pd.DataFrame([''], columns=['Q_Temp_MAX'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)
            val = pd.DataFrame([''], columns=['Q_Prec_cumu'])
            obs_dataframe = pd.concat([obs_dataframe, val], axis=1)

        obs_dataframe.to_csv(save_path, index=False, mode='a')
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] save {save_path} success!')
