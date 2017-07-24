#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: QiBin
# Date : 20170411


def read_table(filename):
    tn = []
    with open(filename, 'r') as l:
        lines = l.readlines()
        for line in lines:
            line = line.strip('\n')
            tn.append(line)
        return tn


def main():
    file_list = read_table('device.txt')
    f = open('device_new.txt', 'w')
    for file in file_list:
        print type(file)
        if 'vid' not in file:
            f1 = file.strip() + "_day\n"
            print f1
            f2 = file.strip() + "_hour\n"
            print f2
            f.write(f1)
            f.write(f2)
        else:
            f1 = file.strip() + '_day\n'
            f.write(f1)
    f.close()


if __name__ == '__main__':
    main()

