import { pack_tf_binary, unpack_tf_binary } from "./BinaryUtils";

describe('BinaryUtils', () => {
    test('pack_tf_binary', () => {
        const tf = 100;
        const len = 200;
        const timestamp = new Uint8Array(6).fill(1); // fill with dummy data

        const result = pack_tf_binary(tf, len, timestamp);

        expect(result.length).toBe(12);
        expect(result[0]).toBe(100 & 0xFF);
        expect(result[1]).toBe((100 >> 8) & 0xFF);
        expect(result[2]).toBe(200 & 0xFF);
        expect(result[3]).toBe((200 >> 8) & 0xFF);
        expect(result[4]).toBe((200 >> 16) & 0xFF);
        expect(result[5]).toBe((200 >> 24) & 0xFF);
        expect(result.slice(6)).toEqual(timestamp);
    });

    test('unpack_tf_binary', () => {

        const current_time_in_millis = Date.now();
        const tf_binary = new Uint8Array([
            100, 0, // term frequency
            200, 0, 0, 0, // document length
            current_time_in_millis & 0xFF,
            (current_time_in_millis >> 8) & 0xFF,
            (current_time_in_millis >> 16) & 0xFF,
            (current_time_in_millis >> 24) & 0xFF,
            (current_time_in_millis >> 32) & 0xFF,
            (current_time_in_millis >> 40) & 0xFF,
        ]);

        

        const { tf, len, timestamp } = unpack_tf_binary(tf_binary);

        expect(tf).toBe(100);
        expect(len).toBe(200);
        expect(timestamp).toBe(current_time_in_millis & 0xFFFFFFFFFFFF); // mask to get the last 48 bits
    });

    test('pack_tf_binary with large values', () => {
        const tf = 70000; // greater than 65535
        const len = 5000000000; // greater than 4294967295
        const timestamp = new Uint8Array(6).fill(1); // fill with dummy data

        const result = pack_tf_binary(tf, len, timestamp);

        expect(result.length).toBe(12);
        expect(result[0]).toBe(65535 & 0xFF); // should truncate to 65535
        expect(result[1]).toBe((65535 >> 8) & 0xFF);
        expect(result[2]).toBe(4294967295 & 0xFF); // should truncate to 4294967295
        expect(result[3]).toBe((4294967295 >> 8) & 0xFF);
        expect(result[4]).toBe((4294967295 >> 16) & 0xFF);
        expect(result[5]).toBe((4294967295 >> 24) & 0xFF);
        expect(result.slice(6)).toEqual(timestamp);
    });

    test('unpack_tf_binary with large values', () => {
        const current_time_in_millis = Date.now();
        const timestamp_value = (current_time_in_millis & 0xFFFFFFFFFFFF) >> 8; // shift to fit in 6 bytes
        const timestamp_bytes = new Uint8Array(6).fill(1); // fill with dummy data
        timestamp_bytes[0] = (timestamp_value & 0xFF);
        timestamp_bytes[1] = (timestamp_value >> 8) & 0xFF;
        timestamp_bytes[2] = (timestamp_value >> 16) & 0xFF;
        timestamp_bytes[3] = (timestamp_value >> 24) & 0xFF;
        timestamp_bytes[4] = (timestamp_value >> 32) & 0xFF;
        timestamp_bytes[5] = (timestamp_value >> 40) & 0xFF;
        const tf_binary = pack_tf_binary(70000, 5000000000, timestamp_bytes);
        // Unpack the binary data

        const { tf, len, timestamp } = unpack_tf_binary(tf_binary);

        expect(tf).toBe(65535);
        expect(len).toBe(4294967295);
        expect(timestamp).toBe(current_time_in_millis & 0xFFFFFFFFFFFF); // mask to get the last 48 bits
    });

    test('pack_tf_binary with zero values', () => {
        const tf = 0;
        const len = 0;
        const timestamp = new Uint8Array(6).fill(0); // fill with zero data

        const result = pack_tf_binary(tf, len, timestamp);

        expect(result.length).toBe(12);
        expect(result[0]).toBe(0); // term frequency
        expect(result[1]).toBe(0);
        expect(result[2]).toBe(0); // document length
        expect(result[3]).toBe(0);
        expect(result[4]).toBe(0);
        expect(result[5]).toBe(0);
        expect(result.slice(6)).toEqual(timestamp);
    });

    test('unpack_tf_binary with zero values', () => {
        const tf_binary = new Uint8Array([
            0, 0, // term frequency
            0, 0, 0, 0, // document length
            0, 0, 0, 0, 0, 0 // timestamp
        ]);

        const { tf, len, timestamp } = unpack_tf_binary(tf_binary);

        expect(tf).toBe(0);
        expect(len).toBe(0);
        expect(timestamp).toBe(0); // since we packed it with all zeros
    });

});