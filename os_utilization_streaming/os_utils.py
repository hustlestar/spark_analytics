import os

import psutil
import time
from psutil import NoSuchProcess


def log_system_status(output_dir="input", file_name="sys_log", ext="csv"):
    processes = [psutil.Process(x) for x in psutil.pids() if x not in [0, 4, 424]]

    new_log_file = "{output_dir}/{file_name}{now}.{ext}".format(output_dir=output_dir,
                                                                file_name=file_name,
                                                                now=time.time(),
                                                                ext=ext)
    with open(new_log_file, "w") as f:
        for p in processes:
            print p
            try:
                log_string = "{pid},{name},{cpu_percent},{memory_percent}\n".format(pid=p.pid, name=p.name(),
                                                                                    cpu_percent=p.cpu_percent(0.1),
                                                                                    memory_percent=p.memory_percent())
                f.write(log_string)
            except NoSuchProcess:
                print "No such process {pid}".format(pid=p.pid)


def clean_old_logs(dir_path="input", file_name="sys_log", ext="csv"):
    now = time.time()
    garbage_files = []
    for x in os.listdir(dir_path):
        if x.startswith(file_name) and x.endswith(ext):
            file_time = x[len(file_name):-len(ext) - 1]  # time in seconds extracted from filename
            if now - float(file_time) > 300:
                garbage_files.append(x)
    print garbage_files
    for g in garbage_files:
        os.remove(os.path.join(dir_path, g))


log_system_status()
clean_old_logs()
