import os
import pickle

include "fileio.pyx"
def check_callables(funcs):
    for ff in funcs:
        if callable(ff[0]) and callable(ff[1]): continue
        else : raise ValueError

def check_strs(strs):
    for s in strs:
        if isinstance(s, str): continue
        else : raise ValueError

cdef bytes encode_item(value, encoded_value, decode, dict type_map_decode):
    cdef bytes dv
    try:
        dv = decode(value)
    except Exception as e:
        raise RuntimeError
    #
    if dv == encoded_value:
        pass
    else:
        raise RuntimeError
    return dv

cpdef update_fields(fix_values0, var_values0, values, dict fix_fields_map_num,):

cpdef encode_fields(key, encoded_key, values, dict fix_fields, dict type_map_decode, tuple default_field):
    cdef tuple tp=default_field
    cdef dict vars={}
    cdef u64 ll=len(fix_fields)+1, size=0, fix_len = len(fix_fields)
    cdef U64 num
    cdef bytes encoded
    #
    encode_item(key, encoded_key, type_map_decode)
    size += len(encoded)
    for field_value in values:
        field, value, encoded_value= field_value
        #
        if value is not None :
            encoded = encode_item(value, encoded_value, type_map_decode)
            size += len(encoded)
        else:
            pass
        #
        try:
            num = fix_fields[field]
        except KeyError:
            vars[field]=encoded
        else:
            PyTuple_SET_ITEM(tp, num.val, encoded_value)
    return tp, vars, get_U64(size)



cdef encode_insert_fields(key, encoded_key,  values, dict fix_fields, u64 fix_len, dict tem_vars):
    if len(values)<fix_fields:
        raise ValueError
    cdef tuple fixs
    cdef dict vars
    cdef u64 i
    fixs, vars = encode_fields(key, encoded_key,values, fix_fields, fix_len, tem_vars, get_all_none_tuple(len(fix_fields)))
    for i in range(fix_len):
        if PyTuple_GET_ITEM(fixs, i) is not None:
            continue
        else:
            raise ValueError
    return fixs, vars

cdef inline encode_update_fields(key, encoded_key, new_fix_values, tuple old_fix_values, dict old_var_values, dict fix_fields, u64 fix_len, dict tem_vars):
    cdef tuple fixs
    cdef dict new_vars
    fixs, new_vars = encode_fields(key, encoded_key, new_fix_values, fix_fields, fix_len, tem_vars, old_fix_values)
    old_var_values.update(new_vars)
    return fixs, old_var_values


cdef class DB:
    cdef readonly :
        str name
        u64 cur_num
        tuple save_paths
    cdef :
        dict path_map_disks
        dict writing_files
        set will_used_kvs
    cpdef pop_caches(self):

    cpdef flush(self):
        filename = f'{self.name}.db.{self.cur_num}'
        data = pickle.dumps(self)
        cdef array rs
        cdef list files
        filename = f'{self.name}.db.{self.cur_num}'
        rs = write_files_background(filename, self.save_paths, data)
        self.writing_files[filename] = set(self.save_paths)
    cpdef fsync(self):
        for filename, paths in self.writing_files.items():
            NotImplemented

    def create_kvs(self,str name, dict type_map_decode, fix_fields, paths, u64 blocksize):
        if name in self.kvs:
            raise ValueError
        check_strs(fix_fields)
        check_strs(type_map_decode.keys())
        check_callables([type_map_decode.values()])
    def

cdef class Kvs:
    cdef readonly:
        str name
        tuple split_keys, block_caches
        u64 cur_num, blocksize, max_type_num
        tuple paths, fix_fields
    cdef:
        dict type_map_decode, type_map_num
        array changed_blocks
        dict fix_fields  # {name:str, FixField }
        dict var_fields  # {name:str, {t:type, count:U64} }

    def __init__(self, str name, dict type_map_decode, fix_fields, paths, u64 blocksize):
        self.paths = paths
        self.cur_num = 0
        self.blocksize = blocksize
        self.block_caches=tuple()
        self.split_keys = tuple()
        self.changed_blocks=array('b')
        #
        cdef U64 i=0
        cdef dict type_map_num={}
        for t in type_map_decode.keys():
            type_map_num[t]=get_U64(i)
        #
        cdef dict field_map_num={}
        for field in fix_fields:
            field_map_num[field]=get_U64(i)
        self.fix_attrs = {}
        self.var_fields = {}
        #
        i=1
        self.type_map_num = {}
        for t in type_map_decode.keys():
            self.type_map_num[t]=get_U64(i)
        self.type_map_decode = type_map_decode
        self.max_type_num = len(type_map_decode)

    cpdef dict group_keys(self, keys, dict d):
        cdef U64 i
        cdef dict caches = {}, reads = {}
        for key in keys:
            key, value = key
            i = self._bisect_key_in(key)
            if self.block_caches[i] is None:
                dict_append(reads, i, key)
            else:
                dict_append(caches, i, key)
        return caches, reads
    cpdef dict group_kvs(self, kvs):
        cdef U64 i
        cdef dict caches={}, reads={}
        cdef char* changes = self.changed_blocks.data.as_chars
        for kv in kvs:
            key, value = kv
            i = self._bisect_key_in(key)
            if changes[i]==1:
                caches = PyTuple_GET_ITEM(self.block_caches, i)
                if caches is None: #未缓存
                    PyTuple_SET_ITEM(self.block_caches, i, {key:value})
                else:
                    if value is not None: # 已经缓存的
                        encode_update_fields()
                    else:
                        try:
                            del caches[key]
                        except KeyError:
                            pass
                dict_append(reads, i, kv)
            else:
                dict_append(caches, i, kv)
        return caches, reads
    cdef inline U64 _bisect_key_in(self, key):
        cdef u64 i
        i = bisect_obj_in(self.block_files, key, 0, len(self.block_files))
        return get_U64(i)
    cpdef insert_kv(self, key, encoded_key, values):
        cdef U64 i
        cdef dict caches, values1
        cdef char * changes = self.changed_blocks.data.as_chars
        cdef tuple encoded_key_values1
        if self.split_keys:
            i = self._bisect_key_in(key)
            caches = PyTuple_GET_ITEM(self.block_caches, i)
            if changes[i]==0:  # 未读取
                if caches is None:  # 未缓存
                    PyTuple_SET_ITEM(self.block_caches, i, {key: (encoded_key,values)})
                    changes[i]=1
                else: # 已缓存
                    try:
                        encoded_key_values1 = caches[key]
                        values1 = encoded_key_values1[1]
                        values1.update(values)
                        caches[key] = (encoded_key, values1)
                    except KeyError:
                        caches[key] = (encoded_key, values1)
            elif changes[i] == -1: # 已读取未更改
                if values is not None:  # 更新
                    caches[key] = values
                else: # 删除
                    try:
                        del caches[key]
                    except KeyError:
                        pass
                changes[i]=1
            else: # 已更改
                try:
                    values0 = caches[key]
                    if values is not None: #更新
                        values0.update(values)
                    else: #删除
                except KeyError: # 不存在该key

    cpdef load_blocks(self, indexes):
        NotImplemented
    cpdef read_blocks(self, indexes):
        cdef U64 i
        for i in indexes:
            if self.block_caches[i] is None:


    cpdef insert_kvs(self, kvs ):
        cdef dict block_map_keys = self.group_keys( kvs, {})

    cpdef insert_or_update_kvs(self, kvs):
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







