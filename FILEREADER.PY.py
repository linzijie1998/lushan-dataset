# -*- coding:utf-8 -*-

from filereader import CSVReader

if __name__ == '__main__':
    filename = r'E:\LushanDataset\CSV\YCCL_L2\2020\1\UPAR_MODI_MOBS_SUOB_LSYW_YCCL_VAIS_L2_20200101_000000_UFMT.csv'
    reader = CSVReader(filename)
    data = reader.read()
    print(data)
