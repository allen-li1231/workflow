def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024. or unit == 'PiB':
            break
        size /= 1024.

    return f"{size:.{decimal_places}f} {unit}"
