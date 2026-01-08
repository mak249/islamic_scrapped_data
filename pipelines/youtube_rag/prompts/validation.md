# STAGE 4: CHUNK VALIDATION PROMPT

Validate each semantic chunk to prevent low-quality data from entering the RAG system.

## ACCEPTANCE CRITERIA
Accept a chunk ONLY if it meeting ALL of these:
1. It expresses ONE complete and cohesive idea or argument.
2. It stands alone (is understandable without context from neighboring chunks).
3. It contains significant factual, logical, or argumentative value.

## REJECTION CRITERIA
Reject immediately if:
- It is primarily emotional speech or venting.
- It contains repetition without adding new information.
- It is an incomplete or broken argument.
- It contains mostly noise/fillers that survived the cleaning stage.
