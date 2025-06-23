
from libc.stdint cimport uint64_t as u64, uint32_t as u32, uint8_t as u8, uint16_t as u16, int32_t as i32,\
    int16_t as i16, int8_t as i8, int64_t as i64, UINT64_MAX, UINT32_MAX, UINT16_MAX, INT64_MAX, INT64_MIN, UINT32_MAX, UINT8_MAX

ctypedef unsigned int uint

from libc.string cimport memcpy, memcmp, memset, strlen, memmove
from libc.stdlib cimport malloc, free, realloc
from cpython.time cimport time as get_time_double
from cpython.mem cimport PyMem_Free,PyMem_Malloc,PyMem_Realloc, PyMem_RawMalloc, PyMem_RawFree

from cpython.list cimport PyList_New, PyList_SetItem, PyList_GetSlice, PyList_GetItem, PyList_GET_ITEM,  PyList_SET_ITEM, PyList_AsTuple
from cpython.ref cimport Py_INCREF, PyTypeObject, Py_TYPE
from cpython.buffer cimport PyBuffer_FromContiguous
from cpython.unicode cimport PyUnicode_Compare, PyUnicode_FromStringAndSize
from cpython.bytes cimport PyBytes_FromString, PyBytes_FromStringAndSize, PyBytes_AsString
from cpython.float cimport PyFloat_FromDouble
from cpython.tuple cimport PyTuple_New, PyTuple_SetItem, _PyTuple_Resize, PyTuple_GET_ITEM, PyTuple_SET_ITEM
from cpython.ref cimport Py_INCREF, Py_DECREF
from cpython.array cimport array, resize, resize_smart, extend_buffer, copy, newarrayobject, clone
from cpython.object cimport Py_SIZE

from libc.stdio cimport FILE, fopen, fclose, fdopen, fread, fwrite, fseek, ftell, fseek, rewind, fflush, SEEK_SET, SEEK_CUR, SEEK_END, feof, ferror

import threading, time, os
import collections

cdef extern from  "_io_thread.c":
    void thread_sleep(int t) nogil
    void thread_yield() nogil
    void fsync_stream(FILE* stream) nogil
    void fsync_fd(int fd) nogil

cdef:
    set empty_set=set()
    list empty_list=[]

cdef inline void array_extend_buffer(array self, void* ptr, u64 size):
    resize_smart(self, Py_SIZE(self) + size)
    memcpy(self.data.as_chars, ptr, size)

cdef u8* py_malloc(u64 size):
    cdef u8* ptr = <u8*>PyMem_Malloc(size)
    if ptr != NULL:
        return ptr
    else:
        raise_mem_err(size)

cdef raise_mem_err(size):
    e = MemoryError(f'alloc {size}bytes fault ')
    e.size = size
    raise e

#-----------------------------------------------------------------------------------------------------------------------
ctypedef object pyfunc()

cdef new_1_list(): return []
cdef new_1_set(): return set()
cdef new_5_list(): return ([],[],[],[],[])
cdef new_3_list(): return ([],[],[])
cdef new_2_list(): return ([],[])
cdef new_1_dict(): return {}
cdef new_U64(): return  U64().set(0)

cdef default_dict_get(dict d, key, pyfunc* default):
    try:
        return d[key]
    except KeyError:
        r = default()
        d[key] = r
        return r

B, Q = 'B', 'Q'

cpdef array new_array(u64 size):
    cdef array arr = array(B)
    resize(arr, size)
    return arr

cpdef u64 bisect_u64_in(u64* arr, u64 v, u64 low, u64 high):
    cdef u64 mid, mi, end=high-1
    while(1):
        mid = (low + high) >> 1
        mi = arr[mid]
        if mi<v:
            low = mid + 1
        elif mi>v:
            high = mid
        else:
            return mid
    return end


ctypedef object cmp_func(object a, object b)

ctypedef fused listuple:
    list
    tuple

cdef u64 bisect_obj_in(listuple items, item, u64 low, u64 high):
    cdef:
        uint mi, low, high, last=high-1
        object cmp_r, mid
        #
    while (low < high):
        mi = (low + high) >> 1
        mid = items[mi]
        if mid < item:
            low = mi + 1
        elif mid > item:
            high = mi
        else:
            return mi
    return last

cdef u64 _bisect_obj_in(listuple items, item, cmp_func* cmp):
    cdef:
        uint mi, low, high
        object cmp_r
        #
    low, high = 0, len(items)
    while (low < high):
        mi = (low + high) >> 1
        mid = items[mi]
        cmp_r = cmp(mid, item)
        if cmp_r is True:
            low = mi + 1
        elif cmp_r is False:
            high = mi
        else:
            return mi
    return len(items)-1
#-----------------------------------------------------------------------------------------------------------------------
cdef class U64:
    cdef readonly u64 val
    cpdef U64 set(self, u64 val):
        self.val = val
        return self

cdef u64 MAX_CACHE_U64 = 1024*1024
cdef tuple cache_u64s=PyTuple_New(MAX_CACHE_U64)
init_cache_u64s()
cdef init_cache_u64s():
    cdef u64 i
    for i in range(MAX_CACHE_U64):
        PyTuple_SET_ITEM(cache_u64s, i, U64().set(i))

cdef get_U64(u64 i):
    if i < MAX_CACHE_U64:
        return PyTuple_GET_ITEM(cache_u64s, i)
    else:
        return U64().set(i)


cdef class Buffer:
    cdef :
        u8* ptr
        u64 size
    cdef Buffer set(self, u8* ptr, u64 size):
        self.ptr, self.size = ptr, size
        return self

cpdef counter_add_1(dict self, key):
    cdef U64 count
    try:
        count = self[key]
        count.val += 1
    except KeyError:
        count = U64().set(1)
        self[key] = count

cpdef counter_sub_1(dict self, key):
    cdef U64 count
    count = self[key]
    count.val -= 1

cpdef dict_append(dict self, key, item):
    cdef list l=default_dict_get(self, key, &new_1_list)
    l.append(item)

cpdef uint size_get_num(u64 size):
    if size<UINT8_MAX:
        return 1
    elif size<UINT16_MAX:
        return 2
    elif size<UINT32_MAX:
        return 4
    else:
        return 8
#-----------------------------------------------------------------------------------------------------------------------
cdef parallel_read(filenames)

#-----------------------------------------------------------------------------------------------------------------------
class MyRuntimeError(RuntimeError):
    pass
