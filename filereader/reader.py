# -*- coding:utf-8 -*-
import csv
import pandas as pd
import numpy as np

header_code_dict = {
    'AWS': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'Pres_sens_HGT',
        'Temp_RH_sens_HGT',
        'Wind_sens_HGT',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'AERM': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'AERM_sens_HGT',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'VIS': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'VIS_sens_HGT',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'YCCL_L3': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'YCCL_sens_HGT',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'RSD': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'RSD_sens_HGT',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Numb_part_diam_clas',
        'Numb_part_velo_clas',
        'Part_diam_clas',
        'Part_velo_clas',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'MRD': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'MRD_sens_HGT',
        'Data_level',
        'Timezone',
        'HGT',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'YCCL_L2': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'YCCL_sens_HGT',
        'Date_level',
        'Timezone',
        'Time_resolution',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'FSD': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'FSD_sens_HGT',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Sample_area',
        'Channel_count',
        'Size_each_bin',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'PRE': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'Software_version',
        'Prec_sens_HGT',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'RRD_Lraw': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'RRD_sens_HGT',
        'Service_version',
        'Device_version',
        'Devi_seri_numb',
        'Bandwidth',
        'Calibration_constant',
        'MMR_data_qual',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ],
    'RRD_Lpro_and_Lave': [
        'Station_name',
        'Country',
        'Province',
        'City',
        'County',
        'Station_ID',
        'LAT',
        'LON',
        'ALT',
        'Station_type',
        'Station_level',
        'Admi_code_CHN',
        'Mete_data_code',
        'Manufacturer_model',
        'RRD_sens_HGT',
        'Service_version',
        'Device_version',
        'Devi_seri_numb',
        'Calibration_constant',
        'MMR_data_qual',
        'Time_AVG',
        'Sampling_rate',
        'Data_level',
        'Timezone',
        'Time_resolution',
        'Obse_begi_DT',
        'Obse_end_DT',
        'Data_crea_DT',
        'Dataset_version'
    ]
}


class CSVReader:
    def __init__(self, filename):
        """
        传入文件名参数
        :param filename: 文件名
        """
        self.filename = filename

    def read(self):
        """
        读取csv文件，并且返回文件头信息和观测数据
        :return: 包含文件头和观测数据的字典
        """
        instrument_name = self.filename.split('/')[-1].split('_')[5]
        if instrument_name == 'YCCL' or instrument_name == 'RRD':
            instrument_name += '_' + self.filename.split('/')[-1].split('_')[7]
        if instrument_name in ['AWS', 'AERM', 'VIS', 'YCCL_L3']:
            return self.one_dime_csv_read(instrument_name)
        elif instrument_name == 'RSD':
            return self.rsd_csv_read()
        elif instrument_name == 'MRD':
            return self.mrd_csv_read()
        elif instrument_name == 'YCCL_L2':
            return self.yccl_l2_csv_read()
        elif instrument_name == 'FSD':
            return self.fsd_csv_read()
        elif instrument_name == 'PRE':
            return self.pre_csv_read()
        elif instrument_name == 'RRD_Lraw':
            return self.rrd_lraw_csv_read()
        elif instrument_name == 'RRD_Lpro' or instrument_name == 'RRD_Lave':
            return self.rrd_lpro_and_lave_csv_read()

    def one_dime_csv_read(self, instrument_name):
        """
        一维数据的文件读取
        :param instrument_name: 设备名
        :return: 包含文件头和观测数据的字典，其中文件头是字典、观测数据数pandas.Dataframe
        """
        header_code = header_code_dict[instrument_name]
        obs_values = []
        with open(self.filename) as f:
            reader = csv.reader(f)
            header_values = next(reader)
            obs_codes = next(reader)
            for row in reader:
                obs_values.append(row)
        header_data = {}
        for i in range(len(header_values)):
            header_data.update({header_code[i]: header_values[i]})
        obs_data = pd.DataFrame(obs_values, columns=obs_codes)
        return {'header': header_data, 'obs': obs_data}

    def rsd_csv_read(self):
        """
        RSD设备的文件读取
        :return: 包含文件头和观测数据的字典，其中文件头是字典、观测数据数pandas.Dataframe, 多维数据是numpy.ndarray
        """
        header_code = header_code_dict['RSD']
        obs_values = []
        with open(self.filename, encoding='utf-8') as f:
            reader = csv.reader(f)
            header_values = next(reader)
            obs_codes = next(reader)
            for row in reader:
                obs_values.append(row)
        header_data = {}
        for i in range(len(header_values)):
            if header_code[i] == 'Part_diam_clas' or header_code[i] == 'Part_velo_clas':
                header_data.update({header_code[i]: header_values[i].split(' ')})
            else:
                header_data.update({header_code[i]: header_values[i]})
        obs_data = []
        true_obs_codes = []
        for code in obs_codes:
            if code != '':
                true_obs_codes.append(code)
        for i in range(len(obs_values)):
            one_day_values = []
            one_day_prec_spec_value = []
            for j in range(len(obs_codes)):
                if obs_codes[j] == 'Prec_spec' or obs_codes[j] == '':
                    one_day_prec_spec_value.append(obs_values[i][j])
                elif obs_codes[j] == 'Q_data':
                    one_day_values.append(np.array(one_day_prec_spec_value).reshape(22, 20))
                    one_day_values.append(obs_values[i][j])
                else:
                    one_day_values.append(obs_values[i][j])
            obs_data.append(one_day_values)
        obs_data = pd.DataFrame(obs_data, columns=true_obs_codes)
        return {'header': header_data, 'obs': obs_data}

    def mrd_csv_read(self):
        """
        MRD设备的文件读取
        :return: 包含文件头和观测数据的字典，其中文件头是字典、观测数据数pandas.Dataframe, 多维数据是numpy.ndarray
        """
        header_code = header_code_dict['MRD']
        obs_values = []
        with open(self.filename, encoding='utf-8') as f:
            reader = csv.reader(f)
            header_values = next(reader)
            obs_codes = next(reader)
            for row in reader:
                obs_values.append(row)
        header_data = {}
        for i in range(len(header_values)):
            if header_code[i] == 'HGT':
                header_data.update({header_code[i]: header_values[i].split(' ')})
            else:
                header_data.update({header_code[i]: header_values[i]})
        obs_data = []
        true_obs_codes = []
        for code in obs_codes:
            if code != '':
                true_obs_codes.append(code)
        for i in range(len(obs_values)):
            one_day_values = []
            one_day_multi_dime_value = []
            for j in range(len(obs_codes)):
                if obs_codes[j] == 'Temp_prof' or obs_codes[j] == 'VAP_prof' or \
                        obs_codes[j] == 'LWC_prof' or obs_codes[j] == 'RH_prof' or obs_codes[j] == '':
                    one_day_multi_dime_value.append(obs_values[i][j])
                elif obs_codes[j] == 'Datetime_402' or obs_codes[j] == 'Datetime_403' or \
                        obs_codes[j] == 'Datetime_404' or obs_codes[j] == 'Q_Temp_ambi':
                    one_day_values.append(np.array(one_day_multi_dime_value))
                    one_day_multi_dime_value.clear()
                    one_day_values.append(obs_values[i][j])
                else:
                    one_day_values.append(obs_values[i][j])
            obs_data.append(one_day_values)
        obs_data = pd.DataFrame(obs_data, columns=true_obs_codes)
        return {'header': header_data, 'obs': obs_data}

    def yccl_l2_csv_read(self):
        """
        云高仪设备二级数据的读取
        :return: 包含文件头和观测数据的字典，其中文件头是字典、观测数据数pandas.Dataframe, 多维数据是numpy.ndarray
        """
        header_code = header_code_dict['YCCL_L2']
        obs_values = []
        with open(self.filename, encoding='utf-8') as f:
            reader = csv.reader(f)
            header_values = next(reader)
            obs_codes = next(reader)
            for row in reader:
                obs_values.append(row)
        header_data = {}
        for i in range(len(header_values)):
            header_data.update({header_code[i]: header_values[i]})
        obs_data = []
        true_obs_codes = []
        for code in obs_codes:
            if code != '':
                true_obs_codes.append(code)
        for i in range(len(obs_values)):
            one_day_values = []
            one_day_multi_dime_value = []
            for j in range(len(obs_codes)):
                if obs_codes[j] == 'BS_prof' or obs_codes[j] == '':
                    one_day_multi_dime_value.append(obs_values[i][j])
                elif obs_codes[j] == 'Q_BS_prof':
                    one_day_values.append(np.array(one_day_multi_dime_value))
                    one_day_multi_dime_value.clear()
                    one_day_values.append(obs_values[i][j])
                else:
                    one_day_values.append(obs_values[i][j])
            obs_data.append(one_day_values)
        obs_data = pd.DataFrame(obs_data, columns=true_obs_codes)
        return {'header': header_data, 'obs': obs_data}

    def fsd_csv_read(self):
        """
        FSD设备数据的读取
        :return: 包含文件头和观测数据的字典，其中文件头是字典、观测数据数pandas.Dataframe, 多维数据是numpy.ndarray
        """
        header_code = header_code_dict['FSD']
        obs_values = []
        with open(self.filename, encoding='utf-8') as f:
            reader = csv.reader(f)
            header_values = next(reader)
            obs_codes = next(reader)
            for row in reader:
                obs_values.append(row)
        header_data = {}
        for i in range(len(header_values)):
            if header_code[i] == 'Size_each_bin':
                header_data.update({header_code[i]: header_values[i].split(' ')})
            else:
                header_data.update({header_code[i]: header_values[i]})
        obs_data = []
        true_obs_codes = []
        for code in obs_codes:
            if code != '':
                true_obs_codes.append(code)
        for i in range(len(obs_values)):
            one_day_values = []
            one_day_multi_dime_value = []
            for j in range(len(obs_codes)):
                if obs_codes[j] == 'Numb_part_chan' or obs_codes[j] == '':
                    one_day_multi_dime_value.append(obs_values[i][j])
                elif obs_codes[j] == 'Numb_conc':
                    one_day_values.append(np.array(one_day_multi_dime_value))
                    one_day_multi_dime_value.clear()
                    one_day_values.append(obs_values[i][j])
                else:
                    one_day_values.append(obs_values[i][j])
            obs_data.append(one_day_values)
        obs_data = pd.DataFrame(obs_data, columns=true_obs_codes)
        return {'header': header_data, 'obs': obs_data}

    def pre_csv_read(self):
        """
        PRE设备文件的读取
        :return: 包含文件头和观测数据的字典，观测数据又包含温度时间、降雨量时间和整点时间的字典，多维数据是numpy.ndarray
        """
        header_code = header_code_dict['PRE']
        obs_values = []
        with open(self.filename, encoding='utf-8') as f:
            reader = csv.reader(f)
            header_values = next(reader)
            obs_codes = next(reader)
            for row in reader:
                obs_values.append(row)
        header_data = {}
        for i in range(len(header_values)):
            if header_code[i] == 'Size_each_bin':
                header_data.update({header_code[i]: header_values[i].split(' ')})
            else:
                header_data.update({header_code[i]: header_values[i]})
        obs_values = np.array(obs_values)
        temp_data = obs_values[:, 0:3]
        for i in range(len(temp_data)):
            if temp_data[i][0] == '':
                temp_data = temp_data[:i, :]
                break
        pre_data = obs_values[:, 3:6]
        for i in range(len(pre_data)):
            if pre_data[i][0] == '':
                pre_data = pre_data[:i, :]
                break
        max_data = obs_values[:, 6:]
        for i in range(len(max_data)):
            if max_data[i][0] == '':
                max_data = max_data[:i, :]
                break
        temp_data = pd.DataFrame(temp_data, columns=obs_codes[0:3])
        pre_data = pd.DataFrame(pre_data, columns=obs_codes[3:6])
        max_data = pd.DataFrame(max_data, columns=obs_codes[6:])

        return {
            'header': header_data,
            'obs': {
                'temp': temp_data,
                'pre': pre_data,
                'max': max_data
            }}

    def rrd_lraw_csv_read(self):
        """
        RRD设备原始数据的文件读取
        :return: 包含文件头和观测数据的字典，其中文件头是字典、观测数据数pandas.Dataframe, 多维数据是numpy.ndarray
        """
        header_code = header_code_dict['RRD_Lraw']
        obs_values = []
        obs_codes = [
            'Datetime',
            'HGT',
            'Transfer_function',
            'Spectral_reflectivities',
            'Q_data'
        ]
        with open(self.filename, encoding='utf-8') as f:
            reader = csv.reader(f)
            header_values = next(reader)
            for row in reader:
                obs_values.append(row)
        header_data = {}
        for i in range(len(header_values)):
            header_data.update({header_code[i]: header_values[i]})
        obs_data = []
        one_day_values = []
        spec_ref_values = []
        for i in range(len(obs_values)):
            code = obs_values[i][0]
            if code == 'Spectral_reflectivities' or code == '':
                spec_ref_values.append(obs_values[i][1:])
            elif code == 'Q_data':
                one_day_values.append(spec_ref_values)
                spec_ref_values = []
                one_day_values.append(obs_values[i][1])
                obs_data.append(one_day_values)
                one_day_values = []
            elif code == 'HGT' or code == 'Transfer_function':
                one_day_values.append(obs_values[i][1:])
            else:
                one_day_values.append(obs_values[i][1])
        obs_data = pd.DataFrame(obs_data, columns=obs_codes)
        return {'header': header_data, 'obs': obs_data}

    def rrd_lpro_and_lave_csv_read(self):
        """
        RRD设备再处理数据和平均数据的文件的读取
        :return: 包含文件头和观测数据的字典，其中文件头是字典、观测数据数pandas.Dataframe, 多维数据是numpy.ndarray
        """
        header_code = header_code_dict['RRD_Lpro_and_Lave']
        obs_values = []
        obs_codes = [
            'Datetime',
            'HGT',
            'Transfer_function',
            'Spectral_reflectivities',
            'Drop_size',
            'Spec_drop_dens',
            'Path_Inte_Atte',
            'Z_Atte',
            'Z_Atte_corr',
            'Rain_rate',
            'LWC',
            'W',
            'Q_data'
        ]
        with open(self.filename, encoding='utf-8') as f:
            reader = csv.reader(f)
            header_values = next(reader)
            for row in reader:
                obs_values.append(row)
        header_data = {}
        for i in range(len(header_values)):
            header_data.update({header_code[i]: header_values[i]})
        obs_data = []
        one_day_values = []
        multi_dime_values = []
        for i in range(len(obs_values)):
            code = obs_values[i][0]
            if code == '':
                multi_dime_values.append(np.array(obs_values[i][1:]))
            elif code == 'Spectral_reflectivities' or code == 'Drop_size' or code == 'Spec_drop_dens':
                if code != 'Spectral_reflectivities':
                    one_day_values.append(np.array(multi_dime_values))
                    multi_dime_values = []
                multi_dime_values.append(np.array(obs_values[i][1:]))
            elif code == 'Path_Inte_Atte':
                one_day_values.append(np.array(multi_dime_values))
                one_day_values.append(np.array(obs_values[i][1:]))
            elif code == 'HGT' or code == 'Transfer_function' or code == 'Z_Atte' or code == 'Z_Atte_corr' or \
                    code == 'Rain_rate' or code == 'LWC' or code == 'W':
                one_day_values.append(np.array(obs_values[i][1:]))
            elif code == 'Q_data':
                one_day_values.append(obs_values[i][1])
                obs_data.append(one_day_values)
                one_day_values = []
            else:
                one_day_values.append(obs_values[i][1])
        obs_data = pd.DataFrame(obs_data, columns=obs_codes)
        return {'header': header_data, 'obs': obs_data}
