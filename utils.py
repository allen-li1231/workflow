import re
import numpy as np
import pandas as pd


def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024. or unit == 'PiB':
            break
        size /= 1024.

    return f"{size:.{decimal_places}f} {unit}"


def reduce_mem_usage(df: pd.DataFrame):
    """
        通过调整数据类型，帮助我们减少数据在内存中占用的空间
    """
    #    start_mem = df.memory_usage().sum() # 初始内存分配
    #    print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))

    for col in df.columns:  # 针对每一列
        col_type = df[col].dtype  # 每一列的数据类型
        if re.findall('float|int', str(col_type)):  # 如果不是object类型的
            c_min = df[col].min()  # 这一列的最小值
            c_max = df[col].max()  # 这一列的最大值

            if str(col_type)[:3] == 'int':  # 如果是int类型的
                # iinfo(type):整数类型的机器限制
                # iinfo(np.int8)-->iinfo(min=-128, max=127, dtype=int8)
                # iinfo(np.int16)-->iinfo(min=-32768, max=32767, dtype=int16)
                # iinfo(np.int32)-->iinfo(min=-2147483648, max=2147483647, dtype=int32)
                # iinfo(np.int64)-->iinfo(min=-9223372036854775808, max=9223372036854775807, dtype=int64)
                # 若c_min大于-128 且c_max小于127，就转换为np.int8类型
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                # finfo(dtype):浮点类型的机器限制
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            continue
    # end_mem = df.memory_usage().sum()
    #    print('Memory usage after optimization is: {:.2f} MB'.format(end_mem)) # 转化后占用内存
    #    print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem)) # 减少的内存
    return df


def read_file_in_chunks(file_object, block_size, chunks=-1):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 25MB."""
    i = 1
    while chunks:
        data = file_object.read(block_size)
        if len(data) < block_size:
            yield -1, data
            return

        yield i, data
        i += 1
        chunks -= 1
