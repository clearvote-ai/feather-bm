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

export function unpack_tf_binary(tf_binary: Uint8Array) : { tf: number, len: number, timestamp: number } 
{
    //first 2 bytes are the term frequency as a uint 16
    const tf = (tf_binary[0] | (tf_binary[1] << 8)) & 0xFFFF; //mask to get the last 16 bits

    //next 4 bytes are the document length as a uint 32
    const len_bytes = new Uint8Array(4);
    len_bytes.set(tf_binary.slice(2, 6));
    const len = (len_bytes[0] | (len_bytes[1] << 8) | (len_bytes[2] << 16) | (len_bytes[3] << 24)); //mask to get the last 32 bits

    //next 6 bytes are the timestamp
    //copy first 6 bytes of the uuid to the tf binary
    const timestamp = (tf_binary[6] | (tf_binary[7] << 8) | (tf_binary[8] << 16) | (tf_binary[9] << 24) | (tf_binary[10] << 32) | (tf_binary[11] << 40)) & 0xFFFFFFFFFFFF; //mask to get the last 48 bits
    
    return { tf, len, timestamp };
}

export function unpack_uuid_binary(uuid_binary: Uint8Array) : string 
{
    //convert the binary uuid to a string
    const uuid = stringify(uuid_binary);
    return uuid;
}

export function pack_uuid_binary(uuid: string) : Uint8Array
{
    //parse the uuid string to a Uint8Array
    const uuid_bytes = parse(uuid);
    return uuid_bytes;
}