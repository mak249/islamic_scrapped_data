# Vedkabhed Pipeline Rules

1. **POSITION**: Sibling pipeline in `pipelines/vedkabhed/`. Isolated.
2. **PURPOSE**: Ingestion + Cleaning + Organization ONLY. No training/inference.
3. **SOURCE IDENTITY**: Preserve original wording, tone, and URL. NEVER rephrase or neutralize.
4. **CLASSIFICATION**: Only if obvious (article|critique|response). Do not infer authority.
5. **FORMAT-AGNOSTIC**: Stream content. Detect format. Skip unsupported.
6. **CLEANING**: HTML/Encoding/UI removal only. No tone softening.
7. **DEDUPLICATION**: Isolated to Vedkabhed.
8. **PROGRESS**: Show exact counts (Processed/Remaining/Speed).
9. **OUTPUT**: `pipelines/vedkabhed/output/` only. One source = one dataset.
10. **RAG SAFETY**: Requires opt-in retrieval. Never include in default Islamic Q&A.
11. **EXECUTION**: Independent. No dependency on IslamQA/Sunnah.
12. **VALIDATION**: Verify URL safety, no cross-contamination.