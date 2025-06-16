include "heads.pyx"

cdef class DB:
    cdef :
        dict kvs, bits

cdef class Kvs:
    cdef readonly:
        str name
        object encode_key, decode_key, encode_value, decode_value, encode_item, decode_item
        u64 blocksize
        tuple split_keys
        str descr
        dict disks

cdef bytes encode_len_prefix(u64 x):
    NotImplemented
    
cdef str _utf8='utf-8'

cpdef encode_item()
def encode_kvs(list kvs, encode_key, encode_item, tuple fix_attrs_type, tuple encode_attrs, bytes encoded_none, encode_var_attr, encode_len_prefix, u64 blocksize):
    cdef list blocks=[], split_keys=[], types=[set() if t==object else t for t in fix_attrs_type ], items=[]
    cdef bytes item, var_name, len_prefidx
    cdef str name
    cdef set ts
    cdef u64 i=0, l=len(kvs), block_size=0, itemsize=0, it, offset=0, attr_len, i_start, ii
    cdef array offsets=array('Q'), itemdata
    cdef u8[9] __
    for i in range(l):
        k, attrs, var_attrs = kvs[i]
        #
        i_start=len(items)
        items.append(None)
        if len(attrs) == len(fix_attrs_type):
            #
            itemdata = array('B')
            #
            for it in range(len(fix_attrs_type)):
                encode_func = encode_attrs[it]
                attr = attrs[it]
                #
                if attr is not None:
                    try:
                        item = encode_func(attr)
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
        #
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
                type_ = fix_attrs_type[it]
                if type_ is object:
                    ts = types[it]
                    ts.add(type(attr))
                #
                attr_len += 1
        #
        itemsize = Py_SIZE(itemdata)
        len_prefidx = encode_len_prefix(itemsize)
        resize(itemdata, itemsize+len(len_prefidx))
        memmove(itemdata.data.as_chars+len_prefidx, itemdata.data.as_chars, itemsize)
        memcpy(itemdata.data.as_chars, PyBytes_AsString(len_prefidx), len(len_prefidx))
        items.append(itemdata)
        #
        block_size += itemsize + len(len_prefidx)
        if block_size < blocksize:
            continue
        else:
            blocks.append(items)
            split_keys.append(k)
            block_size = 0
            offset += block_size
            array_extend_buffer(offsets, &offset, 8)
        #




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







