import os
import threading

include "heads.pyx"
'''cdef dict cache_files #{filename:str, File }
cdef u64 max_cache_file_count = 4000
cpdef File open_file(str filename):
    cdef File file
    try:
        return cache_files[filename]
    except KeyError:
        file = File(filename)
        if len(cache_files) < max_cache_file_count:
            cache_files[filename] = file
        else:
            
        return file'''

cdef class Disk:
    cdef readonly:
        u64 read_speed
        tuple folders
        object io_thread
        volatile int busy
    cdef :
        dict filename_map_path
        volatile ioTask* read_task
    def __init__(self):
        t=threading.Thread(target=start_read_thread, args=(self,), daemon=True)
        t.start()
        self.io_thread=t
    def __dealloc__(self):
        self.busy=2
        while(self.busy!=3): #等待Io线程结束
            continue
    cpdef submit_read(self, IoTask task):
        self.read_task = &task.task
        self.busy = 1
    cpdef submit_write(self, IoTask task):
        self.write_task = &task.task
        self.busy = 1
    cpdef IoTask choose_a_file_read(self,set wait_read_filenames):
        for filename in wait_read_filenames:
            try:
                path = self.filename_map_path[filename]
            except KeyError:
                continue
            fp = os.path.join(path, filename)
            file = File(filename)
            task = file.get_read_task()
            self.submit_read(task)
            self.wait_read_filenames.remove(filename)
            return task
        return None

cpdef start_read_thread(Disk disk):
    _start_read_thread(&disk.read_task, &disk.busy)

cdef _start_read_thread(volatile ioTask* task, volatile int* busy) nogil noexcept:
    cdef u64 read_size
    while (1):
        #等待任务
        while (busy[0]==0):
            continue
        # 退出
        if busy[0]==2:
            break
        # 执行任务
        if fseek(task.stream, 0, SEEK_SET) != 0:
            read_size = fread(task.data, 1, task.size, task.stream)
            task.success = task.size
        else:
            task.success = -1
        busy[0]=0
    busy[0]=3


cdef struct ioTask:
    FILE* stream
    u8* data
    u64 size
    volatile int success

cdef class IoTask:
    cdef readonly :
        File file
    cdef ioTask task

    def __lt__(self,IoTask other):
        return self.task.size < other.task.size
    def __gt__(self,IoTask other):
        return self.task.size > other.task.size
    def __le__(self,IoTask other):
        return self.task.size <= other.task.size
    def __ge__(self,IoTask other):
        return self.task.size >= other.task.size
    def __dealloc__(self):
        self.file = None
        self.task.stream = NULL

class MyIoError(IOError):
    NotImplemented

cdef class File:
    cdef readonly :
        str filename
        int fd
    cdef FILE* stream
    def __init__(self, str filename):
        self.filename = filename
        try:
            fd = os.open(filename, os.O_RDWR)
        except Exception as e:
            raise RuntimeError

        cdef FILE* stream = fdopen(fd, c"rb+")
        if stream != NULL:
            self.stream = stream
        else:
            raise RuntimeError

    cpdef u64 filelen(self):
        NotImplemented
    cpdef IoTask get_read_task(self):
        cdef IoTask task= IoTask()
        cdef u64 size = self.filelen()
        cdef u8* data = py_malloc(size)
        task.file = self
        task.task = ioTask(self.stream, data, 0, size, 0)
        return task
    def __dealloc__(self):
        self.stream=NULL
        self.fd = -1
        self.filename = None

#-----------------------------------------------------------------------------------------------------------------------

cdef class ReadingTasks:
    cdef:
        ioTask** ctasks
        list tasks
        array ciotasks
    cdef readonly:
        u64 cur, count
    cdef set(self, list tasks, array ciotasks):
        assert Py_SIZE(ciotasks)*ciotasks.desrc.itemsize == len(tasks) * sizeof(ioTask)
        self.ciotasks, self.tasks, self.count = ciotasks, tasks, len(tasks)
        self.ctasks = <ioTask**>ciotasks.data.as_ulonglongs
        self.cur = 0
    def __iter__(self):
        return self
    def __next__(self):
        cdef ioTask* task
        cdef int success
        if self.cur < self.count:
            task = self.ctasks[self.cur]
            success = task.success
            if success==1:
                r = self.tasks[self.cur]
            elif success==0:
                r = None
            else:
                r = MyIoError(task.success)
            self.cur += 1
            return r
        else:
            raise StopIteration

cdef class IoPool:
    cdef readonly :
        tuple disks
    cdef:
        set wait_read_filenames
    cpdef submit_read(self):
        cdef Disk disk
        cdef File file
        cdef IoTask task
        cdef list tasks=[]
        for disk in self.disks:
            if disk.busy==0:
                for filename in self.wait_read_filenames:
                    try:
                        path = disk.filename_map_path[filename]
                    except KeyError:
                        continue
                    fp = os.path.join(path, filename)
                    file = File( filename )
                    task = file.get_read_task()
                    disk.submit_read(task)
                    tasks.append(task)
                    self.wait_read_filenames.remove(filename)
                    break
            else:
                continue
        return tasks


cdef u64 sum_iotasks_size(iotasks):
    cdef u64 size=0
    cdef IoTask task
    for task in iotasks:
        size += task.task.size
    return size

cdef u64 sum_disk_read_speeds(list disks):
    cdef u64 speed=0
    cdef Disk disk
    for disk in disks:
        speed += disk.read_speed
    return speed

cdef array assign_disks_read(list disks, u64 total_speed, u64 total_size, list iotasks):
    cdef Disk disk
    cdef u64 suggest_read_size, cur_read_size
    cdef uint i, i_task=0, pre_itask=0, ii
    cdef list disk_tasks, tasks=[]
    cdef array ciotasks
    cdef ioTask* ciotask_ptr
    cdef IoTask task
    for i in range(len(disks)):
        disk = disks[i]
        suggest_read_size = <u64>(total_size*(disk.read_speed/total_speed))
        #
        disk_tasks=[]
        cur_read_size = 0
        for i_task in range(pre_itask, len(iotasks)):
            task = iotasks[i_task]
            disk_tasks.append(task)
            cur_read_size += task.task.size
            if cur_read_size < suggest_read_size:
                continue
            else:
                pre_itask = i_task+1
                if cur_read_size:
                    ciotasks = array(Q)
                    resize(ciotasks, len(disk_tasks))
                    ciotask_ptr = ciotasks.data.as_ulonglongs
                    for ii in range(len(disk_tasks)):
                        ciotask_ptr[i] = <u64*>&task.task
                    reading = ReadingTasks().set(disk_tasks, ciotasks, len(disk_tasks))
                    tasks.append(reading)
                    break
                else:
                    return tasks
    assert i_task == len(iotasks)
    return tasks

cdef parallel_read(list iotasks, list disks):
    cdef u64 total_size = sum_iotasks_size(iotasks), total_speed = sum_disk_read_speeds(disks)
    cdef uint i
    cdef IoTask task
    cdef ReadingTasks reading
    cdef list tasks
    tasks = assign_disks_read(disks, total_speed, total_size)




