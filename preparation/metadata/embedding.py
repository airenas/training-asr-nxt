import json
from typing import Dict


def parse_emb(emb_bytes: bytes):
    res: Dict[str, str] = {}
    text = bytes(emb_bytes).decode("utf-8")

    for line in text.splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        speaker = data["sp"]
        res[speaker] = data["emb"]
    return res


def load_embeddings(conn, file_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT content FROM kv WHERE id = %s AND type = 'speaker.embeddings.jsonl'",
            (file_id,)
        )
        emb_row = cur.fetchone()
    if emb_row:
        lang_bytes = emb_row[0]
        return parse_emb(bytes(lang_bytes))
    return None


def get_speaker_embedding(conn, file_id, speaker):
    embs = load_embeddings(conn=conn, file_id=file_id)
    if not embs:
        return None
    for spk, emb in embs.items():
        if spk == speaker:
            return emb
    return None
