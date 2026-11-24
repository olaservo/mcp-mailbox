import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { openSeq, privFromRaw, sealSeq, setupR, setupS, type AeadId } from "../src/reader/hpke.ts";

interface Vector {
  aead_id: AeadId;
  info: string;
  skEm: string;
  skRm: string;
  pkRm: string;
  enc: string;
  key: string;
  base_nonce: string;
  encryptions: { seq: number; aad: string; pt: string; ct: string }[];
}

// From test-vectors.json in cfrg/draft-irtf-cfrg-hpke: mode 0, KEM 0x20, KDF 1, and the sequence numbers RFC 9180 prints.
// AEAD 1 and 3 are RFC 9180 A.1.1 and A.2.1.
const vectors = JSON.parse(readFileSync(new URL("./hpke-vectors.json", import.meta.url), "utf8")) as Vector[];
const hex = (text: string) => Buffer.from(text, "hex");

for (const v of vectors) {
  test(`HPKE base mode matches the CFRG vector for AEAD ${v.aead_id}`, () => {
    const sender = setupS(v.aead_id, hex(v.pkRm), hex(v.info), { privateKey: privFromRaw(hex(v.skEm)) });
    const receiver = setupR(v.aead_id, hex(v.enc), privFromRaw(hex(v.skRm)), hex(v.info));
    assert.equal(sender.enc.toString("hex"), v.enc);
    for (const context of [sender, receiver]) {
      assert.equal(context.key.toString("hex"), v.key);
      assert.equal(context.baseNonce.toString("hex"), v.base_nonce);
    }
    for (const e of v.encryptions) {
      assert.equal(sealSeq(v.aead_id, sender, e.seq, hex(e.aad), hex(e.pt)).toString("hex"), e.ct);
      assert.equal(openSeq(v.aead_id, receiver, e.seq, hex(e.aad), hex(e.ct)).toString("hex"), e.pt);
      assert.throws(() => openSeq(v.aead_id, receiver, e.seq, hex(`${e.aad}00`), hex(e.ct)), "another aad does not open");
    }
    assert.throws(() => setupR(v.aead_id, hex(`${v.enc}00`), privFromRaw(hex(v.skRm)), hex(v.info)), "an enc of 33 bytes");
  });
}
