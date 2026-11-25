"""Encode a BEIR corpus with ColBERTv2 for `kosha-late-interaction-eval` (#205).

Writes <out>/docs.jsonl, <out>/queries.jsonl ({"id", "tokens": [[f32; 128], ...]})
and <out>/qrels.tsv, the input `kosha-late-interaction-eval eval --corpus <out>` reads.

Usage: colbert_encode_beir.py --out DIR [--dataset scifact] [--split test]

Needs `torch`, `transformers`, `huggingface_hub` and `pandas` (CPU is enough
for SciFact: 5,183 passages). The encoder reproduces ColBERTv2's
(`colbert-ir/colbertv2.0`) checkpoint settings without the colbert-ai package:
BERT, then a 768 -> 128 linear layer without bias, then L2 normalization.

* Query: `[CLS] [unused0] tokens [SEP]` padded to 32 with `[MASK]`
  (mask augmentation, not attended to), all 32 output rows kept.
* Passage: `[CLS] [unused1] title text [SEP]`, truncated to 180 tokens;
  padding and punctuation tokens dropped.
"""
import argparse
import json
import os
import string

import pandas as pd
import torch
from huggingface_hub import hf_hub_download
from safetensors.torch import load_file
from transformers import BertModel, BertTokenizerFast

MODEL = "colbert-ir/colbertv2.0"
QUERY_MAXLEN = 32
DOC_MAXLEN = 180
Q_MARKER = "[unused0]"
D_MARKER = "[unused1]"


class ColBERT(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.bert = BertModel.from_pretrained(MODEL, add_pooling_layer=False)
        self.linear = torch.nn.Linear(768, 128, bias=False)
        weights = load_file(hf_hub_download(MODEL, "model.safetensors"))
        self.linear.weight.data.copy_(weights["linear.weight"])

    def forward(self, ids, mask):
        out = self.linear(self.bert(input_ids=ids, attention_mask=mask).last_hidden_state)
        return torch.nn.functional.normalize(out, dim=-1)


def encode(model, tok, texts, marker, maxlen, query, batch):
    marker_id = tok.convert_tokens_to_ids(marker)
    skip = {tok.convert_tokens_to_ids(c) for c in string.punctuation} if not query else set()
    for start in range(0, len(texts), batch):
        enc = tok(
            texts[start : start + batch],
            padding="max_length" if query else "longest",
            truncation=True,
            max_length=maxlen - 1,
            return_tensors="pt",
        )
        ids = torch.cat([enc.input_ids[:, :1], torch.full_like(enc.input_ids[:, :1], marker_id), enc.input_ids[:, 1:]], 1)
        mask = torch.cat([enc.attention_mask[:, :1], enc.attention_mask[:, :1], enc.attention_mask[:, 1:]], 1)
        if query:
            ids[ids == tok.pad_token_id] = tok.mask_token_id
        with torch.inference_mode():
            emb = model(ids, mask)
        for row in range(ids.shape[0]):
            if query:
                keep = torch.ones(ids.shape[1], dtype=torch.bool)
            else:
                keep = mask[row].bool() & torch.tensor([int(t) not in skip for t in ids[row]])
            yield emb[row][keep]


def write_bags(path, ids, bags):
    with open(path, "w") as f:
        for i, bag in zip(ids, bags):
            tokens = [[round(x, 5) for x in v] for v in bag.tolist()]
            f.write(json.dumps({"id": str(i), "tokens": tokens}, separators=(",", ":")) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--dataset", default="scifact")
    ap.add_argument("--split", default="test")
    ap.add_argument("--batch", type=int, default=32)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    torch.set_num_threads(os.cpu_count() or 1)

    repo = f"BeIR/{args.dataset}"
    corpus = pd.read_parquet(hf_hub_download(repo, "corpus/corpus-00000-of-00001.parquet", repo_type="dataset"))
    queries = pd.read_parquet(hf_hub_download(repo, "queries/queries-00000-of-00001.parquet", repo_type="dataset"))
    qrels = pd.read_csv(
        hf_hub_download(f"{repo}-qrels", f"{args.split}.tsv", repo_type="dataset"), sep="\t", dtype=str
    )
    queries = queries[queries["_id"].astype(str).isin(set(qrels["query-id"]))]
    qrels.to_csv(os.path.join(args.out, "qrels.tsv"), sep="\t", index=False)

    tok = BertTokenizerFast.from_pretrained(MODEL)
    model = ColBERT().eval()
    q_texts = queries["text"].tolist()
    write_bags(
        os.path.join(args.out, "queries.jsonl"),
        queries["_id"].tolist(),
        encode(model, tok, q_texts, Q_MARKER, QUERY_MAXLEN, True, args.batch),
    )
    d_texts = [f"{t} {x}".strip() for t, x in zip(corpus["title"].fillna(""), corpus["text"].fillna(""))]
    write_bags(
        os.path.join(args.out, "docs.jsonl"),
        corpus["_id"].tolist(),
        encode(model, tok, d_texts, D_MARKER, DOC_MAXLEN, False, args.batch),
    )
    print(f"wrote {len(corpus)} passages, {len(queries)} queries to {args.out}")


if __name__ == "__main__":
    main()
