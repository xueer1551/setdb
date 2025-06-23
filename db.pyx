import os
import pickle

include "heads.pyx"
def check_callables(funcs):
    for ff in funcs:
        if callable(ff[0]) and callable(ff[1]): continue
        else : raise ValueError

def check_strs(strs):
    for s in strs:
        if isinstance(s, str): continue
        else : raise ValueError
cdef class DB:
    cdef readonly :
        pass
    cdef :
        dict path_map_disks
        dict kvs
    def create_kvs(self,str name, dict type_map_encode_decode, fix_fields, paths, u64 blocksize):
        if name in self.kvs:
            raise ValueError
        check_strs(fix_fields)
        check_strs(type_map_encode_decode.keys())
        check_callables([type_map_encode_decode.values()])

cdef class Kvs:
    cdef readonly:
        str name
        tuple split_keys, block_caches
        u64 cur_num,
        u64 blocksize,
        tuple paths
    cdef:
        dict type_map_encode_decode
        array changed
        dict fix_fields  # {name:str, FixField }
        dict var_fields  # {name:str, {t:type, count:U64} }

    def __init__(self, str name, dict type_map_encode_decode, fix_fields):
        cdef U64 i=0
        cdef dict type_map_num={}
        for t in type_map_encode_decode.keys():
            type_map_num[t]=get_U64(i)
        cdef dict field_map_num={}
        for field in fix_fields:
            field_map_num[field]=get_U64(i)

        self.fix_attrs = {}
    cpdef dict group_keys(self, keys, dict d):
        cdef U64 i
        for key in keys:
            i = self._bisect_key_in(key)
            dict_append(d, i, key)
        return d
    cdef inline U64 _bisect_key_in(self, key):
        cdef u64 i = bisect_obj_in(self.block_files, key, 0, len(self.block_files))
        return get_U64(i)
    cpdef load_blocks(self, indexes):
        NotImplemented
    cpdef read_blocks(self, indexes):
        cdef U64 i
        for i in indexes:
            if self.block_caches[i] is None:


    cpdef insert_kvs(self, keys, values, fields):
        NotImplemented
    cpdef insert_or_update_kvs(self, keys, values):
        for value in values:
            if len(value) >=
        NotImplemented
    cpdef update_kvs(self, keys, values, fields):
        NotImplemented
    cpdef deleted_kvs(self, kvs):
        NotImplemented
    cpdef deleted_or_ignore_kvs(self, kvs):
        NotImplemented
    cpdef get_kvs(self, kvs):
        NotImplemented



cdef class FixField:
    cdef readonly:
        str name
        u64 max_num
    cdef:
        dict type_map_encode_and_num, type_map_count
    cpdef encode(self, obj):
        try:
            encode = self.type_map_encode_and_num[type(obj)]
            r = encode(obj)
        except KeyError:
            r = pickle.dumps(obj)
        counter_add_1(self.type_map_count, type(obj))
        return r


cdef class BlockMeta:
    cdef readonly:
        u64 uint_size, uint_count


#-----------------------------------------------------------------------------------------------------------------------

cdef class DB:
    cdef :
        dict kvs, bits

cdef class Kvs:
    cdef readonly:
        str name
        object encode_key, decode_key, encode_kv, decode_kv
        u64 blocksize
        tuple split_keys, cache_block
        str descr
    cdef:
        dict fix_attrs # {name:str, set[type]|type }
        dict var_attrs # {name:str, {t:type, count:U64} }
        dict disks


cdef bytes encode_len_prefix(u64 x):
    NotImplemented

cdef u64 decode_len_prefix(const u8* view, u64* prefix_len):
    NotImplemented

cdef str _utf8='utf-8'

cpdef bytes encode_item(value, encode, decode):
    cdef bytes view = encode(value)
    v = decode(view)
    if v == value:
        return view
    else:
        raise RuntimeError

cpdef decode_item(const u8[:] view, u64 offset, decode, Buffer buf):
    cdef u64 prefix_len, size = decode_len_prefix(&view[0]+offset, &prefix_len)
    if prefix_len+size+offset <= view.nbytes:
        buf.set(&view[0]+offset+prefix_len, size)
        return decode(buf)
    else:
        raise RuntimeError

cpdef decode_items(const u8[:] view, decode ):
    cdef u64 offset = 0
    cdef Buffer buffer = Buffer()
    cdef list rs=[]
    try:
        while (offset < view.nbytes):
            item = decode_item(view, offset, decode, buffer)
            rs.append(item)
            offset = buffer.ptr-&view[0]
    except Exception as e:
        raise e
    if offset == view.nbytes:
        return rs
    else:
        raise RuntimeError

cpdef array join_arrays(list datas, bint add_len_prefix=True, bint add_count_prefix=False):
    cdef u64 size = 0, offset, i=0
    cdef u8[:] data
    cdef array data1=array('B')
    cdef bytes len_prefix
    for data in datas:
        size += data.nbytes
    #
    if add_len_prefix or add_count_prefix:
        if add_len_prefix:
            len_prefix = encode_len_prefix(size, data1.data.as_uchars + offset)
        else:
            len_prefix = encode_len_prefix(len(datas), data1.data.as_uchars + offset)
        size += len(len_prefix)
        data1.resize(size)
        memcpy(data1.data.as_uchars + offset, <char*>len_prefix, len(len_prefix))
        offset = len(len_prefix)
    else:
        data1.resize(size)
        offset = 0
    #
    for i in range(len(datas)):
        data = datas[i]
        memcpy(data1.data.as_uchars + offset, &data[0], data.nbytes)
    return data1

def encode_kvs(list kvs, encode_key, encode_item, tuple fix_attrs_type, tuple encode_attrs, bytes encoded_none, encode_var_attr, encode_len_prefix, u64 blocksize):
    cdef list blocks=[], split_keys=[], types=[set() if t==object else t for t in fix_attrs_type ], items=[]
    cdef bytes item, var_name, len_prefidx, item_key
    cdef str name
    cdef set ts
    cdef u64 i=0, l=len(kvs), block_size=0, itemsize=0, it, offset=0, attr_len, ii, itemlen
    cdef U64 count
    cdef dict vars={}, type_map_count
    cdef array offsets=array('Q'), itemdata, filedata, keysdata, filemeta
    for i in range(l):
        k, attrs, var_attrs = kvs[i]
        itemdata = array('B')
        # encode key
        try:
            item_key = encode_item(k, encode_key)
            array_extend_buffer( itemdata, <char*>item_key, len(item_key) )
        except Exception as e:
            raise RuntimeError from e
        # encode 固定字段
        if len(attrs) == len(fix_attrs_type):
            #
            for it in range(len(fix_attrs_type)):
                encode_func = encode_attrs[it]
                attr = attrs[it]
                #
                if attr is not None:
                    try:
                        item = encode_item(attr, encode_func)
                        array_extend_buffer(itemdata, PyBytes_AsString(item), len(item))
                    except Exception as e:
                        raise RuntimeError from e
                    #
                    type_ = fix_attrs_type[it]
                    if type_ is object:
                        ts = types[it]
                        ts.add( type(attr) )
                else:
                    item = encoded_none
            attr_len = len(attrs)
        else:
            raise RuntimeError
        # encode 可变字段
        if var_attrs:
            for name, attr in var_attrs:
                try:
                    var_name = name.encode(_utf8)
                    array_extend_buffer(itemdata, PyBytes_AsString(var_name), len(var_name))
                    item = encode_var_attr(attr)
                    array_extend_buffer(itemdata, PyBytes_AsString(item), len(item))
                except Exception as e:
                    raise RuntimeError from e
                #
                type_map_count = default_dict_get(vars, name, &new_1_dict)
                count = default_dict_get(type_map_count, type(attr), &new_U64)
                count.val += 1
                #
                attr_len += 1
        # 组装一个字段
        itemsize = Py_SIZE(itemdata)
        len_prefidx = encode_len_prefix(itemsize)
        itemlen = itemsize + len(len_prefidx)
        # memmove
        resize(itemdata, itemlen)
        for ii in range(1,itemsize+1):
            itemdata.data.as_chars[itemlen-ii] = itemdata.data.as_chars[itemsize-ii]
        # 写入长度前缀
        memcpy(itemdata.data.as_chars, PyBytes_AsString(len_prefidx), len(len_prefidx))
        items.append(itemdata)
        # 判断当前是否够一个block
        block_size += itemlen
        if block_size < blocksize:
            continue
        else:
            blocks.append(items)
            split_keys.append(item_key)
            block_size = 0
            offset += block_size
            array_extend_buffer(offsets, &offset, 8)
        #
    filedata = join_arrays(items)
    keysdata = join_arrays(split_keys)
    assert offset == Py_SIZE(filedata)
    assert len(split_keys)==len(offsets)
    filemeata = join_arrays([ keysdata, offsets])
    #
    items.clear()
    return filedata, filemeata, types, vars, keysdata, offsets

cpdef write_kvs(filedata, filemeta, types, vars, filepath, list rs, size_t i):
    fn = f'{filepath}.ing'
    try:
        f=open(fn, 'wbx')
        f1 = open(f'{filepath}.data', 'wbx')
        f2 = open(f'{filepath}.meta', 'wbx')
        #
        f1.write(filedata)
        f1.flush()
        #
        f2.write(filemeta)
        f2.flush()
        #
        os.fsync(f1.fileno())
        os.fsync(f2.fileno())
        f1.close()
        f2.close()
        #
        f.close()
        os.remove(fn)
        rs[i] = True
    except Exception as e:
        if os.path.exists(fn):
            os.remove(fn)
        rs[i] = e


cdef class NewKvsTask:
    cdef readonly:
        str filename
        tuple paths, kvs
        object encode_key, decode_key, encode_value, decode_value, encode_item, decode_item
        u64 blocksize
    cdef:
        list split_keys, encoded_items, kvs
        array split_offsets0
    def __next__(self):
        cdef u64 l=len(self.kvs), i=1,size=0
        cdef bytes item
        while(l>=i):
            k, v = self.kvs[l-i]
            try:
                item = self.encode_item(k,v)
            except Exception as e:
                raise RuntimeError from e
































cdef class DB:
    cdef :
        list triggers_dag # [(src:Kvs, src_operator:str, dst:Kvs, dst_operator:str)]
        dict kv_name_map_dag_node
        set head_triggers
        dict kvs, deleted_kvs, indexes,  # {str: Kvs}
        set disks # {str: Disk}

    def delete_kvs(self, str name):
        if name in self.deleted_kvs:
            raise RuntimeError
        elif name in self.kvs:
            self.deleted_kvs[name] = self.kvs.pop(name)
        else:
            raise ValueError

    def remove_kvs(self, str name):
        if name in self.kvs:
            NotImplemented
        else:
            raise ValueError

    def recovery_kvs(self, str name):
        if name in self.delete_kvs:
            self.kvs[name] = self.deleted_kvs.pop(name)
        else:
            raise ValueError

    cpdef Kvs new_kvs(self, str name, tuple disk_paths, encode_key, decode_key, encode_value, decode_value, encode_item, decode_item, update_value, blocksize, key_types, str fix_attrs,  str desrc):
        cdef set disks
        NotImplemented
        kvs = Kvs(name, disk_paths, encode_key, decode_key, encode_value, decode_value, encode_item, decode_item, update_value, blocksize, key_types, fix_attrs, desrc)
        self.kvs[name] = kvs
        return kvs

    cpdef Kvs create_kvs(self, str name, tuple disk_paths, key_type, value_atts, value_type, str descr=None):
        NotImplemented






cdef class SetItemAttr:
    cdef readonly :
        str name
        object encode, decode, encode_item, decode_item
        object types # str|({(type:Type, count:int),……},……)

cdef class Set:
    cdef readonly :
        str name


cdef class Kvs:
    cdef readonly :
        str name
        object encode_key, decode_key, encode_value, decode_value, encode_item, decode_item, update_value
        u64 cur_num, changed_version, blocksize
        tuple split_file_keys
        str descr
        dict disks

    cdef init(self, str name, tuple disk_paths, encode_key, decode_key, encode_value, decode_value, encode_item,
                 decode_item, update_value, blocksize, key_types, dict fix_attrs,  str descr):
        self.name = name
        self.disk_paths = dict([(disk,os.path.join(path, name)) for disk, path in disk_paths.items()])
        self.encode_key = encode_key
        self.decode_key = decode_key
        self.encode_value = encode_value
        self.decode_value = decode_value
        self.encode_item = encode_item
        self.decode_item = decode_item
        self.update_value = update_value
        self.blocksize = blocksize
        self.key_types = key_types
        self.fix_attrs = fix_attrs
        self.descr = descr

cdef class SetData(Kvs):
    cdef :
        object key_types # str | dict
        list fix_attrs, var_attrs # [SetItemAttr,……]
        dict attrs # {name: str, attr:SetItemAttr}
        array filenums, delted_ids
    def __init__(self, str name, tuple disk_paths, encode_key, decode_key, encode_value, decode_value, encode_item,
                 decode_item, update_value, blocksize, key_types, dict fix_attrs,  str descr):
        self.init(name, disk_paths, encode_key, decode_key, encode_value, decode_value, encode_item, decode_item, update_value, blocksize, key_types, fix_attrs, descr)
        #
        self.var_attrs = {}
        self.attrs = dict(self.fix_attrs.items())
        self.split_file_keys=()
        self.cur_num = self.changed_version = 0
        self.filenums = array('Q')
        self.delted_ids = array('Q')

cdef class SetIndex(Kvs):
    NotImplemented




cdef class KvsIncrease:
    cdef readonly :
        Kvs old_kvs, new_kvs
    cdef :
        array deleted_ids
        dict cache_file







