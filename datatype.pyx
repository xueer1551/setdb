include "heads.pyx"

cdef class Struct:
    cdef readonly:
        tuple fields_types
        u64 fixed_size
        array lens
    def __init__(self, tuple fields_types):
        NotImplemented


cdef uint bit_len(u64 v):
    cdef uint bitlen=0
    cdef u64 vv = v, vvv=v
    while (vv > 0):
        vvv = vv
        vv >>= 8
        bitlen += 8
    while (vvv > 0):
        vv = vvv
        vvv >>= 1
        bitlen += 1
    return bitlen

cdef u64 mask_8byte = 255<<56, mask_64bit = 1<<63

cdef uint u64_after_encode_size(u64 v):
    cdef uint bitlen = bit_len(v), bytecount=<uint>(bitlen/7)
    return bytecount+1 if bitlen%7>0 else bytecount
cdef u8* encode_u64(u64 v, u8* buf,):
    cdef :
        u64 vv, vvv, flag
        uint shange, yushu, bitlen, bc, highest_bit, bytecount
    if v&mask_8byte==0:
        bitlen = bit_len(v)
        if bitlen>0:
            shang = bitlen>>3
            yushu = bitlen&7
            bc= shang+1 if yushu>0 else shang #v的值至少需要bc个byte储存
            #
            if yushu+shang<=8:
                highest_bit = bc<<3
            else:
                highest_bit = (bc<<3)+8 #v压缩后需要用highest_bit个bit表示
            flag = 1<<shang #最高位开始000……1一共有多少位(n个0和1个1)，就代表这个数有多少个字节(n+1)
            v |= highest_bit-shang
            #
            assert highest_bit&7==0
            bytecount = highest_bit>>3
            #
            memcpy(buf, &v, bytecount)
            return buf+bytecount
        else:
            buf[0]=0
            return buf+1
    else:
        memcpy(buf, &v, 8)
        buf[8]=0
        return buf+9


cdef u64 decode_u64(u8 * data, u64* data_bytecount, u8** next_data):
    cdef:
        u64 bytecount
        u64 c = <u64> (data[0]),
        u64 mask = 128, value_bytecount, cpy_bytecount
        u64 v
        u8 * cs = <u8 *> &v
        u8 high_byte
    if c > 0: #这个压缩的数小于8个字节
        bytecount = 1
        while (c & mask == 0):
            mask >>= 1
            bytecount += 1
        assert bytecount < 9
        high_byte = c & (mask - 1)
        if high_byte!=0: #当前位有效
            cpy_bytecount = bytecount-1
            cs[cpy_bytecount]=high_byte
            memcpy(cs, data - cpy_bytecount, cpy_bytecount)
            next_data[0] = data - bytecount
            data_bytecount[0] = bytecount
        else: #当前位无效
            memcpy(cs, data -1 - bytecount, bytecount)
            next_data[0] = data -( bytecount + 1)
            data_bytecount[0] = bytecount-1
    else: #这个压缩的数有8个字节
        memcpy(cs, data, 8)
        next_data[0] = data - 9
        data_bytecount[0]=8
    return v

#-----------------------------------------------------------------------------------------------------------------------