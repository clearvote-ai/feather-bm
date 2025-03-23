import { parse, stringify } from 'uuid';

const MAX_UINT16 = 65535;
const MAX_UINT32 = 4294967295;

export function pack_tf_binary(tf: number, len: number, timestamp: Uint8Array) : Uint8Array 
{
    const tf_binary = new Uint8Array(12);

    //if the term frequency is greater than 65535, truncate it to 65535
    const tf_truncated = Math.min(tf, 65535);

    //first 2 bytes are the term frequency
    tf_binary.set([tf_truncated & 0xFF, (tf_truncated >> 8) & 0xFF], 0);

    //truncate it to 4294967295 and convert it to uint32
    const len_truncated = Math.min(len, 4294967295); //truncate to 32 bits
    const len_bytes = new Uint8Array(4);
    len_bytes[0] = len_truncated & 0xFF;
    len_bytes[1] = (len_truncated >> 8) & 0xFF;
    len_bytes[2] = (len_truncated >> 16) & 0xFF;
    len_bytes[3] = (len_truncated >> 24) & 0xFF;
    tf_binary.set(len_bytes, 2);

    //next 6 bytes are the timestamp
    tf_binary.set(timestamp.slice(0, 6), 6);
    return tf_binary;
}

export function unpack_tf_binary(data: Uint8Array) : { tf: number, len: number, timestamp: Uint8Array }
{
    if (data.length !== 12) {
        throw new Error("Invalid data length, expected 12 bytes");
    }
    const tf = data[0] | (data[1] << 8);
    const len = (data[2] | (data[3] << 8) | (data[4] << 16) | (data[5] << 24)) >>> 0; // ensure unsigned interpretation
    const timestamp = data.slice(6);
    return { tf, len, timestamp };
}

export function pack_uuid_binary(uuid: string) : Uint8Array
{
    //parse the uuid string to a Uint8Array
    const uuid_bytes = parse(uuid);
    return uuid_bytes;
}