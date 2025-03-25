import { pack_tf_binary, unpack_tf_binary } from "./BinaryUtils";

describe('BinaryUtils', () => {
    test('pack_tf_binary', () => {
        const tf = 100;
        const len = 200;

        const result = pack_tf_binary(tf, len);

        expect(result.length).toBe(6);
        expect(result[0]).toBe(100 & 0xFF);
        expect(result[1]).toBe((100 >> 8) & 0xFF);
        expect(result[2]).toBe(200 & 0xFF);
        expect(result[3]).toBe((200 >> 8) & 0xFF);
        expect(result[4]).toBe((200 >> 16) & 0xFF);
        expect(result[5]).toBe((200 >> 24) & 0xFF);
    });

    test('unpack_tf_binary', () => {
        const tf_binary = new Uint8Array([
            100, 0, // term frequency
            200, 0, 0, 0, // document length
        ]);
        
        const { tf, len } = unpack_tf_binary(tf_binary);

        expect(tf).toBe(100);
        expect(len).toBe(200);
    });

    test('pack_tf_binary with large values', () => {
        const tf = 70000; // greater than 65535
        const len = 5000000000; // greater than 4294967295

        const result = pack_tf_binary(tf, len);

        expect(result.length).toBe(6);
        expect(result[0]).toBe(65535 & 0xFF); // should truncate to 65535
        expect(result[1]).toBe((65535 >> 8) & 0xFF);
        expect(result[2]).toBe(4294967295 & 0xFF); // should truncate to 4294967295
        expect(result[3]).toBe((4294967295 >> 8) & 0xFF);
        expect(result[4]).toBe((4294967295 >> 16) & 0xFF);
        expect(result[5]).toBe((4294967295 >> 24) & 0xFF);
    });

    test('unpack_tf_binary with large values', () => {
        //pack the binary data with large values
        const tf_binary = pack_tf_binary(70000, 5000000000);
        
        // Unpack the binary data
        const { tf, len } = unpack_tf_binary(tf_binary);

        expect(tf).toBe(65535);
        expect(len).toBe(4294967295);
    });

    test('pack_tf_binary with zero values', () => {
        const tf = 0;
        const len = 0;

        const result = pack_tf_binary(tf, len);

        expect(result.length).toBe(6);
        expect(result[0]).toBe(0); // term frequency
        expect(result[1]).toBe(0);
        expect(result[2]).toBe(0); // document length
        expect(result[3]).toBe(0);
        expect(result[4]).toBe(0);
        expect(result[5]).toBe(0);
    });

    test('unpack_tf_binary with zero values', () => {
        const tf_binary = new Uint8Array([
            0, 0, // term frequency
            0, 0, 0, 0, // document length
        ]);

        const { tf, len } = unpack_tf_binary(tf_binary);

        expect(tf).toBe(0);
        expect(len).toBe(0);
    });

});